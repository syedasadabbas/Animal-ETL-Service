"""
ETL System for Animal Data Processing
Fetches, transforms, and loads animal data from API
"""

import logging
import requests
import time
from datetime import datetime
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import RequestException, HTTPError
import pytz

BASE_URL = "http://localhost:3123"
BATCH_SIZE = 100

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_process.log')
    ]
)
logger = logging.getLogger(__name__)


class ETLStats:
    """Track ETL process statistics"""
    def __init__(self):
        self.start_time = time.time()
        self.animals_found = 0
        self.animals_processed = 0
        self.animals_posted = 0
        self.batches_posted = 0
        self.errors = []
        
    def duration(self) -> float:
        return time.time() - self.start_time
        
    def add_error(self, error: str):
        self.errors.append(error)
        logger.error(error)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((RequestException, HTTPError))
)
def fetch_animals_page(page: int) -> Dict[str, Any]:
    """Fetch a single page of animals with retry logic"""
    try:
        response = requests.get(
            f"{BASE_URL}/animals/v1/animals",
            params={"page": page},
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout on page {page}, retrying...")
        raise
    except requests.exceptions.HTTPError as e:
        if e.response.status_code >= 500:
            logger.warning(f"Server error on page {page}, retrying...")
            raise
        else:
            logger.error(f"HTTP error on page {page}: {e}")
            raise


def fetch_all_animal_ids() -> List[int]:
    """Fetch all animal IDs from paginated API"""
    logger.info("Starting to fetch all animal IDs...")
    animal_ids = []
    page = 1
    
    while True:
        try:
            data = fetch_animals_page(page)
            
            if not data.get('items'):
                break
                
            page_ids = [item['id'] for item in data['items'] if 'id' in item]
            animal_ids.extend(page_ids)
            
            total_pages = data.get('total_pages', '?')
            logger.info(f"Page {page}/{total_pages}: Found {len(page_ids)} animals")
            
            if data.get('total_pages') and page >= data['total_pages']:
                break
                
            page += 1
            
        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            raise

    unique_ids = list(set(animal_ids))
    logger.info(f"Total unique animals found: {len(unique_ids)}")
    return unique_ids


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((RequestException, HTTPError))
)
def fetch_animal_details(animal_id: int) -> Dict[str, Any]:
    """Fetch detailed information for a specific animal"""
    try:
        response = requests.get(
            f"{BASE_URL}/animals/v1/animals/{animal_id}",
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching animal {animal_id}, retrying...")
        raise
    except requests.exceptions.HTTPError as e:
        if e.response.status_code >= 500:
            logger.warning(f"Server error fetching animal {animal_id}, retrying...")
            raise
        else:
            logger.error(f"HTTP error fetching animal {animal_id}: {e}")
            raise


def transform_animal(animal: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform animal data:
    1. Convert friends from comma-delimited string to array
    2. Convert born_at to ISO8601 UTC timestamp
    """
    transformed = animal.copy()

    if "friends" in transformed and transformed["friends"]:
        if isinstance(transformed["friends"], str):
            friends_list = [f.strip() for f in transformed["friends"].split(",") if f.strip()]
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
                dt = datetime.fromisoformat(transformed["born_at"].replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=pytz.UTC)
                else:
                    dt = dt.astimezone(pytz.UTC)
            
            transformed["born_at"] = dt.isoformat()
            
        except Exception as e:
            logger.warning(f"Failed to transform born_at for animal {animal.get('id', 'unknown')}: {e}")
    
    return transformed


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((RequestException, HTTPError))
)
def post_animals_batch(batch: List[Dict[str, Any]]) -> None:
    """POST a batch of animals to the home endpoint"""
    if len(batch) > 100:
        raise ValueError(f"Batch size {len(batch)} exceeds maximum of 100")
    
    try:
        logger.info(f"Posting batch of {len(batch)} animals...")
        response = requests.post(
            f"{BASE_URL}/animals/v1/home",
            json=batch,
            timeout=60,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        logger.info(f"Successfully posted batch of {len(batch)} animals")
        
    except requests.exceptions.Timeout:
        logger.warning("Timeout posting batch, retrying...")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error posting batch: {e}")
        if hasattr(e.response, 'text'):
            logger.error(f"Response: {e.response.text}")
        raise


def run_etl_process() -> ETLStats:
    """Run the complete ETL process"""
    stats = ETLStats()
    logger.info("Starting ETL process...")
    
    try:
        animal_ids = fetch_all_animal_ids()
        stats.animals_found = len(animal_ids)
        
        if not animal_ids:
            logger.warning("No animals found to process")
            return stats
        
        batch = []
        
        for i, animal_id in enumerate(animal_ids, 1):
            try:
                animal_details = fetch_animal_details(animal_id)
                
                transformed_animal = transform_animal(animal_details)
                batch.append(transformed_animal)
                stats.animals_processed += 1
                
                if len(batch) >= BATCH_SIZE or i == len(animal_ids):
                    post_animals_batch(batch)
                    stats.animals_posted += len(batch)
                    stats.batches_posted += 1
                    batch = []
                
                if i % 100 == 0:
                    logger.info(f"Processed {i}/{len(animal_ids)} animals...")
                    
            except Exception as e:
                error_msg = f"Failed to process animal {animal_id}: {e}"
                stats.add_error(error_msg)
                continue
        
        logger.info(f"ETL process completed in {stats.duration():.2f} seconds")
        logger.info(f"Processed {stats.animals_processed}/{stats.animals_found} animals")
        logger.info(f"Posted {stats.animals_posted} animals in {stats.batches_posted} batches")
        
        if stats.errors:
            logger.warning(f"Encountered {len(stats.errors)} errors during processing")
            
    except Exception as e:
        error_msg = f"ETL process failed: {e}"
        stats.add_error(error_msg)
        logger.error(error_msg)
    
    return stats


def main():
    """Main entry point"""
    print("="*60)
    print("Animal ETL Process")
    print("="*60)
    
    stats = run_etl_process()
    
    print("\n" + "="*60)
    print("ETL PROCESS SUMMARY")
    print("="*60)
    print(f"Duration: {stats.duration():.2f} seconds")
    print(f"Animals Found: {stats.animals_found}")
    print(f"Animals Processed: {stats.animals_processed}")
    print(f"Animals Posted: {stats.animals_posted}")
    print(f"Batches Posted: {stats.batches_posted}")
    
    if stats.errors:
        print(f"\nErrors ({len(stats.errors)}):")
        for error in stats.errors[-5:]:
            print(f"  - {error}")
    
    success_rate = (stats.animals_processed / stats.animals_found * 100) if stats.animals_found > 0 else 0
    print(f"\nSuccess Rate: {success_rate:.1f}%")
    
    if success_rate >= 95:
        print("✅ ETL process completed successfully!")
    else:
        print("⚠️  ETL process completed with issues")
    
    input("\nPress any key to exit...")


if __name__ == "__main__":
    main()