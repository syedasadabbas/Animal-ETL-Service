"""
Unit Tests for Animal ETL System
"""

import unittest
from unittest.mock import Mock, patch

from requests.exceptions import HTTPError

from .utils.etl_service import (
    ETLError,
    ETLStats,
    fetch_paginated_animals,
    get_animal_details,
    is_server_error,
    post_animals_batch,
    transform_animal,
)


class TestETLStats(unittest.TestCase):
    """Test ETL statistics tracking"""

    def test_etl_stats_initialization(self):
        stats = ETLStats()
        self.assertEqual(stats.total_animals_found, 0)
        self.assertEqual(stats.animals_processed, 0)
        self.assertEqual(stats.animals_posted, 0)
        self.assertEqual(stats.batches_posted, 0)
        self.assertEqual(len(stats.errors), 0)
        self.assertEqual(stats.current_step, "Initializing")

    def test_add_error(self):
        stats = ETLStats()
        stats.add_error("Test error")
        self.assertEqual(len(stats.errors), 1)
        self.assertIn("Test error", stats.errors[0])

    def test_complete(self):
        stats = ETLStats()
        stats.complete()
        self.assertEqual(stats.current_step, "Completed")
        self.assertIsNotNone(stats.end_time)


class TestAnimalTransformation(unittest.TestCase):
    """Test animal data transformation logic"""

    def test_transform_friends_string_to_array(self):
        """Test friends field transformation from string to array"""
        animal = {"id": 1, "name": "Buddy", "friends": "Max,Luna,Charlie"}

        transformed = transform_animal(animal)

        self.assertEqual(transformed["friends"], ["Max", "Luna", "Charlie"])

    def test_transform_friends_empty_string(self):
        """Test friends field with empty string"""
        animal = {"id": 1, "name": "Buddy", "friends": ""}

        transformed = transform_animal(animal)

        self.assertEqual(transformed["friends"], [])

    def test_transform_friends_missing_field(self):
        """Test animal without friends field"""
        animal = {"id": 1, "name": "Buddy"}

        transformed = transform_animal(animal)

        self.assertEqual(transformed["friends"], [])

    def test_transform_friends_with_whitespace(self):
        """Test friends field with extra whitespace"""
        animal = {"id": 1, "name": "Buddy", "friends": " Max , Luna , Charlie "}

        transformed = transform_animal(animal)

        self.assertEqual(transformed["friends"], ["Max", "Luna", "Charlie"])

    def test_transform_born_at_timestamp(self):
        """Test born_at transformation from timestamp"""
        timestamp = 1609459200
        animal = {"id": 1, "name": "Buddy", "born_at": timestamp}

        transformed = transform_animal(animal)

        self.assertTrue(transformed["born_at"].endswith("+00:00"))
        self.assertIn("2021-01-01", transformed["born_at"])

    def test_transform_born_at_milliseconds(self):
        """Test born_at transformation from milliseconds timestamp"""
        timestamp_ms = 1609459200000
        animal = {"id": 1, "name": "Buddy", "born_at": timestamp_ms}

        transformed = transform_animal(animal)

        self.assertTrue(transformed["born_at"].endswith("+00:00"))
        self.assertIn("2021-01-01", transformed["born_at"])

    def test_transform_born_at_iso_string(self):
        """Test born_at transformation from ISO string"""
        animal = {"id": 1, "name": "Buddy", "born_at": "2021-01-01T00:00:00Z"}

        transformed = transform_animal(animal)

        self.assertTrue(transformed["born_at"].endswith("+00:00"))

    def test_transform_born_at_empty(self):
        """Test born_at transformation with empty value"""
        animal = {"id": 1, "name": "Buddy", "born_at": None}

        transformed = transform_animal(animal)

        self.assertEqual(transformed["born_at"], None)

    def test_transform_preserves_other_fields(self):
        """Test that transformation preserves other fields"""
        animal = {
            "id": 1,
            "name": "Buddy",
            "species": "Dog",
            "age": 3,
            "friends": "Max,Luna",
        }

        transformed = transform_animal(animal)

        self.assertEqual(transformed["id"], 1)
        self.assertEqual(transformed["name"], "Buddy")
        self.assertEqual(transformed["species"], "Dog")
        self.assertEqual(transformed["age"], 3)


class TestAPIFunctions(unittest.TestCase):
    """Test API interaction functions"""

    @patch("etl.utils.etl_service.requests.get")
    def test_get_animal_details_success(self, mock_get):
        """Test successful animal details fetch"""
        mock_response = Mock()
        mock_response.json.return_value = {"id": 1, "name": "Buddy", "species": "Dog"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = get_animal_details(1)

        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "Buddy")
        mock_get.assert_called_once_with(
            "http://localhost:3123/animals/v1/animals/1", timeout=30
        )

    @patch("etl.utils.etl_service.requests.get")
    def test_get_animal_details_404(self, mock_get):
        """Test animal details with 404 error"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        mock_get.return_value.raise_for_status.side_effect = HTTPError(
            response=mock_response
        )

        with self.assertRaises(ETLError) as context:
            get_animal_details(999)

        self.assertIn("not found", str(context.exception))

    @patch("etl.utils.etl_service.requests.post")
    def test_post_animals_batch_success(self, mock_post):
        """Test successful batch posting"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        batch = [{"id": 1, "name": "Buddy"}]
        result = post_animals_batch(batch)

        self.assertEqual(result, 200)
        mock_post.assert_called_once()

    def test_post_animals_batch_too_large(self):
        """Test batch size validation"""
        large_batch = [{"id": i} for i in range(101)]

        with self.assertRaises(ETLError) as context:
            post_animals_batch(large_batch)

        self.assertIn("exceeds maximum", str(context.exception))

    @patch("etl.utils.etl_service.fetch_page_with_retry")
    def test_fetch_paginated_animals_single_page(self, mock_fetch):
        """Test pagination with single page"""
        mock_fetch.return_value = {"items": [{"id": 1}, {"id": 2}], "total_pages": 1}

        result = fetch_paginated_animals()

        self.assertEqual(result, [1, 2])
        mock_fetch.assert_called_once_with(1)

    @patch("etl.utils.etl_service.fetch_page_with_retry")
    def test_fetch_paginated_animals_multiple_pages(self, mock_fetch):
        """Test pagination with multiple pages"""

        def side_effect(page):
            if page == 1:
                return {"items": [{"id": 1}, {"id": 2}], "total_pages": 2}
            elif page == 2:
                return {"items": [{"id": 3}, {"id": 4}], "total_pages": 2}
            else:
                return {"items": [], "total_pages": 2}

        mock_fetch.side_effect = side_effect

        result = fetch_paginated_animals()

        self.assertEqual(set(result), {1, 2, 3, 4})
        self.assertEqual(mock_fetch.call_count, 2)

    @patch("etl.utils.etl_service.fetch_page_with_retry")
    def test_fetch_paginated_animals_empty_response(self, mock_fetch):
        """Test pagination with no animals"""
        mock_fetch.return_value = {"items": [], "total_pages": 0}

        result = fetch_paginated_animals()

        self.assertEqual(result, [])


class TestErrorHandling(unittest.TestCase):
    """Test error handling and retry logic"""

    def test_is_server_error_500(self):
        """Test server error detection for 500"""
        mock_response = Mock()
        mock_response.status_code = 500
        error = HTTPError()
        error.response = mock_response

        self.assertTrue(is_server_error(error))

    def test_is_server_error_404(self):
        """Test server error detection for 404 (should be False)"""
        mock_response = Mock()
        mock_response.status_code = 404
        error = HTTPError()
        error.response = mock_response

        self.assertFalse(is_server_error(error))

    def test_is_server_error_non_http_error(self):
        """Test server error detection for non-HTTP errors"""
        error = ValueError("Some other error")

        self.assertFalse(is_server_error(error))


class TestIntegration(unittest.TestCase):
    """Integration tests for complete workflows"""

    @patch("etl.utils.etl_service.post_animals_batch")
    @patch("etl.utils.etl_service.get_animal_details")
    @patch("etl.utils.etl_service.fetch_paginated_animals")
    def test_complete_etl_workflow(self, mock_fetch_ids, mock_get_details, mock_post):
        """Test complete ETL workflow with mocked API calls"""
        from etl.utils.etl_service import run_etl_process

        mock_fetch_ids.return_value = [1, 2]

        def mock_details(animal_id):
            return {
                "id": animal_id,
                "name": f"Animal {animal_id}",
                "species": "Dog",
                "friends": "Buddy,Max",
                "born_at": 1609459200,
            }

        mock_get_details.side_effect = mock_details

        mock_post.return_value = 200

        with patch("etl.utils.etl_service.save_animals_to_db"):
            stats = run_etl_process(batch_size=2)

        self.assertEqual(stats.total_animals_found, 2)
        self.assertEqual(stats.animals_processed, 2)
        self.assertEqual(stats.current_step, "Completed")
        mock_post.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
