import logging
from datetime import datetime
from typing import Any, Dict, List

import pytz
import requests
from django.utils import timezone
from requests.exceptions import HTTPError, RequestException
from tenacity import (retry, retry_if_exception, retry_if_exception_type,
                      stop_after_attempt, wait_exponential)

from etl.models import Animal, APIErrorLog, ETLProcessingLog

logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:3123"


def is_server_error(exc):
    return (
        isinstance(exc, requests.exceptions.HTTPError)
        and exc.response.status_code >= 500
    )


@retry(wait=wait_exponential(min=2, max=60), retry=retry_if_exception(is_server_error))
def fetch_page_with_retry(page):
    resp = requests.get(
        f"{BASE_URL}/animals/v1/animals", params={"page": page}, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


class ETLError(Exception):
    """Custom exception for ETL operations"""

    pass


class ETLStats:
    """Track ETL process statistics"""

    def __init__(self):
        self.reset()

    def reset(self):
        self.total_animals_found = 0
        self.animals_processed = 0
        self.animals_posted = 0
        self.batches_posted = 0
        self.errors = []
        self.start_time = timezone.now()
        self.end_time = None
        self.current_step = "Initializing"

    def add_error(self, error: str):
        self.errors.append(f"{timezone.now()}: {error}")

    def complete(self):
        self.end_time = timezone.now()
        self.current_step = "Completed"

    def get_duration(self):
        end = self.end_time or timezone.now()
        return (end - self.start_time).total_seconds()


etl_stats = ETLStats()


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((RequestException, HTTPError)),
)
def fetch_paginated_animals() -> List[int]:
    """
    Fetch all animal IDs from paginated API endpoint.
    Retries failed pages indefinitely until success.
    """
    animal_ids = []
    page = 1
    total_pages = None

    etl_stats.current_step = "Fetching animal list"
    logger.info("Starting to fetch paginated animals")

    while True:
        try:
            data = fetch_page_with_retry(page)

            if isinstance(data, dict) and "items" in data:
                items = data["items"]
                total_pages = data.get("total_pages", total_pages)

                if not items:
                    break

                page_ids = [item["id"] for item in items if "id" in item]
                animal_ids.extend(page_ids)

                logger.info(
                    f"Page {page}/{total_pages or '?'}: Found {len(page_ids)} animals"
                )

                if total_pages and page >= total_pages:
                    break

                page += 1
            else:
                raise ETLError(f"Unexpected API response structure on page {page}")

        except Exception as e:
            logger.error(f"Fatal error fetching page {page}: {e}")
            etl_stats.add_error(f"Page {page} failed permanently: {str(e)}")
            raise

    unique_ids = list(set(animal_ids))
    etl_stats.total_animals_found = len(unique_ids)
    logger.info(f"Total unique animals found: {len(unique_ids)}")
    return unique_ids


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((RequestException, HTTPError)),
)
def get_animal_details(animal_id: int) -> Dict[str, Any]:
    """
    Fetch detailed information for a specific animal.

    Args:
        animal_id (int): The ID of the animal

    Returns:
        Dict[str, Any]: Animal details

    Raises:
        ETLError: If request fails or animal not found
    """
    try:
        logger.debug(f"Fetching details for animal {animal_id}")
        resp = requests.get(f"{BASE_URL}/animals/v1/animals/{animal_id}", timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching animal {animal_id}, retrying...")
        raise
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"Animal {animal_id} not found")
            raise ETLError(f"Animal {animal_id} not found")
        elif e.response.status_code >= 500:
            logger.warning(f"Server error fetching animal {animal_id}, retrying...")
            raise
        logger.error(f"HTTP error fetching animal {animal_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching animal {animal_id}: {e}")
        raise ETLError(f"Failed to fetch animal {animal_id}: {str(e)}")


def transform_animal(animal: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform animal data according to requirements:
    1. Convert friends from comma-delimited string to array
    2. Convert born_at timestamp to ISO8601 UTC format

    Args:
        animal (Dict[str, Any]): Raw animal data

    Returns:
        Dict[str, Any]: Transformed animal data
    """
    transformed = animal.copy()

    if "friends" in transformed and transformed["friends"]:
        if isinstance(transformed["friends"], str):
            friends_list = [
                f.strip() for f in transformed["friends"].split(",") if f.strip()
            ]
            transformed["friends"] = friends_list
    else:
        transformed["friends"] = []

    if "born_at" in transformed and transformed["born_at"]:
        try:
            if isinstance(transformed["born_at"], (int, float)):
                timestamp = transformed["born_at"]
                if timestamp > 1e10:
                    timestamp = timestamp / 1000
                dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
            elif isinstance(transformed["born_at"], str):
                dt = datetime.fromisoformat(
                    transformed["born_at"].replace("Z", "+00:00")
                )
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=pytz.UTC)
                else:
                    dt = dt.astimezone(pytz.UTC)
            else:
                logger.warning(
                    f"Unexpected born_at format: {type(transformed['born_at'])}"
                )
                dt = None

            if dt:
                transformed["born_at"] = dt.isoformat()
        except Exception as e:
            logger.warning(f"Failed to transform born_at field: {e}")

    return transformed


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((RequestException, HTTPError)),
)
def post_animals_batch(batch: List[Dict[str, Any]]) -> int:
    """
    POST a batch of animals to the home endpoint.

    Args:
        batch (List[Dict[str, Any]]): List of transformed animals (max 100)

    Returns:
        int: HTTP status code

    Raises:
        ETLError: If request fails or batch is too large
    """
    if len(batch) > 100:
        raise ETLError(f"Batch size {len(batch)} exceeds maximum of 100")

    try:
        logger.info(f"Posting batch of {len(batch)} animals")
        resp = requests.post(
            f"{BASE_URL}/animals/v1/home",
            json=batch,
            timeout=60,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        etl_stats.batches_posted += 1
        etl_stats.animals_posted += len(batch)
        logger.info(f"Successfully posted batch of {len(batch)} animals")
        return resp.status_code
    except requests.exceptions.Timeout:
        logger.warning("Timeout posting batch, retrying...")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error posting batch: {e}")
        if hasattr(e.response, "text"):
            logger.error(f"Response content: {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error posting batch: {e}")
        raise ETLError(f"Failed to post batch: {str(e)}")


def save_animals_to_db(animals_data: List[Dict[str, Any]]) -> None:
    """
    Save animals to database using bulk operations for performance.
    Only saves animals that don't already exist (based on api_id).

    Args:
        animals_data: List of tuples (raw_data, transformed_data)
    """
    if not animals_data:
        return

    api_ids = [data["raw"]["id"] for data in animals_data]
    existing_ids = set(
        Animal.objects.filter(api_id__in=api_ids).values_list("api_id", flat=True)
    )

    new_animals = []
    for data in animals_data:
        raw = data["raw"]
        transformed = data["transformed"]

        if raw["id"] not in existing_ids:
            new_animals.append(
                Animal(
                    api_id=raw.get("id"),
                    name=raw.get("name", "Unknown"),
                    species=raw.get("species", "Unknown"),
                    age=raw.get("age"),
                    friends_raw=raw.get("friends", ""),
                    born_at_raw=raw.get("born_at"),
                    friends=transformed.get("friends", []),
                    born_at=transformed.get("born_at"),
                    is_processed=True,
                    is_sent_to_home=True,
                )
            )

    if new_animals:
        Animal.objects.bulk_create(new_animals, batch_size=100, ignore_conflicts=True)
        logger.info(f"Saved {len(new_animals)} new animals to database")


def run_etl_process(batch_size: int = 100) -> ETLStats:
    """
    Run the complete ETL process with independent DB insertion and posting.
    All fetched animals are processed and posted regardless of DB presence.

    Returns:
        ETLStats: Runtime statistics object.
    """
    etl_stats.reset()
    etl_log = ETLProcessingLog.objects.create(status="running")

    try:
        logger.info("Starting ETL process")
        etl_stats.current_step = "Fetching animal IDs"

        all_ids = list(set(fetch_paginated_animals()))
        etl_stats.total_animals_found = len(all_ids)
        etl_log.total_animals_fetched = len(all_ids)

        if not all_ids:
            logger.warning("No animals found to process")
            etl_stats.current_step = "No data found"
            etl_log.status = "completed"
            etl_log.process_end = timezone.now()
            etl_log.save()
            return etl_stats

        etl_stats.current_step = "Processing animals"

        posting_batch = []
        db_batch = []

        logger.info(f"Processing all {len(all_ids)} animals")

        for idx, animal_id in enumerate(all_ids, start=1):
            try:
                raw_details = get_animal_details(animal_id)
                transformed_details = transform_animal(raw_details)

                posting_batch.append(transformed_details)
                db_batch.append(
                    {"raw": raw_details, "transformed": transformed_details}
                )

                etl_stats.animals_processed += 1

                if len(posting_batch) >= batch_size:
                    etl_stats.current_step = (
                        f"Posting batch {etl_stats.batches_posted + 1}"
                    )
                    post_animals_batch(posting_batch)

                    save_animals_to_db(db_batch)

                    posting_batch = []
                    db_batch = []

                if idx % 50 == 0:
                    logger.info(f"Processed {idx}/{len(all_ids)} animals")

            except ETLError as e:
                logger.error(f"ETL error processing animal {animal_id}: {e}")
                etl_stats.add_error(f"Animal {animal_id}: {str(e)}")
                APIErrorLog.objects.create(
                    endpoint=f"/animals/v1/animals/{animal_id}",
                    error_type="ETLError",
                    error_message=str(e),
                )
            except Exception as e:
                logger.error(f"Unexpected error processing animal {animal_id}: {e}")
                etl_stats.add_error(f"Animal {animal_id}: {str(e)}")
                APIErrorLog.objects.create(
                    endpoint=f"/animals/v1/animals/{animal_id}",
                    error_type=type(e).__name__,
                    error_message=str(e),
                )

        if posting_batch:
            etl_stats.current_step = f"Posting final batch"
            post_animals_batch(posting_batch)
            save_animals_to_db(db_batch)

        etl_stats.complete()
        logger.info(
            f"ETL process completed successfully. Processed {etl_stats.animals_processed}/{etl_stats.total_animals_found} animals in {etl_stats.get_duration():.2f} seconds"
        )
        etl_log.status = "completed" if not etl_stats.errors else "partial"

    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        etl_stats.add_error(f"Process failed: {str(e)}")
        etl_stats.current_step = "Failed"
        etl_log.status = "failed"

    etl_log.total_animals_processed = etl_stats.animals_processed
    etl_log.total_animals_sent = etl_stats.animals_posted
    etl_log.errors_encountered = "\n".join(etl_stats.errors[:1000])
    etl_log.process_end = timezone.now()
    etl_log.save()

    return etl_stats
