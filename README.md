# Animal-ETL-Service
A robust ETL pipeline for processing animal data from an unstable API with Django backend and standalone script options.

## Project Setup

1. **Prerequisites**:
   - Python 3.8+
   - Docker


2. **API Setup**:
   ```bash
   docker load -i lp-programming-challenge-1-1625619994.tar.gz
   docker run --rm -p 3123:3123 -ti lp-programming-challenge-1```


3. **Create Virtual Environment**:
   **For Windows**
   Create a virtual environment:
  ```
  python -m venv venv
  ```
  Activate the virtual environment:
  ```
  venv\Scripts\activate
  ```

  **For Linux/macOS:**
  Create a virtual environment:
  ```
  python3 -m venv venv
  ```
  Activate the virtual environment:
  ```
  source venv/bin/activate
  ```


4. **Django Installation**:
  ```
  pip install -r requirements.txt
  python manage.py migrate
  python manage.py runserver
```


5. **ETL RUN**:

Django version:
  ```
  python manage.py run_etl --batch-size=100
  ```
  Standalone:
  ```
  python animal_etl.py
  ```

# Thought Process & Design
## Core Architecture
### Simplified ETL workflow
def run_etl_process():

      ids = fetch_all_animal_ids()          # Extraction
    
      transformed = transform_batch(ids)    # Transformation
    
      post_batches(transformed)             # Loading

### Key Design Decisions:
Modular Components: Separated extraction, transformation, and loading

1. Retry Mechanisms: Handle API instability
2. Batching: Process data in chunks (default 100)
3. Persistence: Store raw+transformed data for auditing
4. Async Processing: Web triggers use background threads

## Pain Points & Solutions
### 1. API Random Pauses (5-15s)
**Solution**: Exponential backoff retry logic
```
@retry(
    wait=wait_exponential(min=2, max=60),
    retry=retry_if_exception(is_server_error)
)
def fetch_page_with_retry(page):
    resp = requests.get(..., timeout=30)
    resp.raise_for_status()
    return resp.json()
```

### 2. HTTP 500, 502, 503 or 504 Errors
**Solution**: Retry on server errors + error logging
```
def is_server_error(exc):
    return isinstance(exc, HTTPError) and exc.response.status_code >= 500

@retry(
    stop=stop_after_attempt(5),
    retry=retry_if_exception(is_server_error)
)
def get_animal_details(animal_id):
    # API call with error handling
```

### 3. Data Transformation Challenges
**Solution**: Type-agnostic field processing
```
def transform_animal(animal):
    # Handle multiple born_at formats
    if isinstance(animal["born_at"], (int, float)):
        dt = datetime.fromtimestamp(animal["born_at"]/1000)
    elif isinstance(animal["born_at"], str):
        dt = datetime.fromisoformat(animal["born_at"].replace('Z', '+00:00'))
    
    # Convert friends string to array
    animal["friends"] = [f.strip() for f in animal["friends"].split(",")]
    return animal
```

### 4. Large Dataset Handling
**Solution**: Batch processing + memory management
```
batch = []
for animal_id in all_ids:
    # Process and accumulate
    if len(batch) >= BATCH_SIZE:
        post_animals_batch(batch)
        batch = []
```

## Additional Features
### 1. Real-time Dashboard:
```
def etl_dashboard(request):
    animals = Animal.objects.order_by("-processed_at")
    return render(request, "etl_dashboard.html", {"animals": animals})
```
### ETL DASHBOARD
**Task Progress**:
![Screenshot 2025-06-06 223451](https://github.com/user-attachments/assets/b3227f92-e9fb-4324-9e44-da7b37367b0d)

**Animal Data Table: (Extracted from db)**:
![Screenshot 2025-06-06 223504](https://github.com/user-attachments/assets/0efb1317-71e5-4719-8900-69f26a859bf5)

**Pie Chart to shpw progressed data numbers and remaining numbers**:
![Screenshot 2025-06-06 223516](https://github.com/user-attachments/assets/a6f41261-8239-45bb-94e0-2295204bdfae)

### 2. Comprehensive Logging:
```
class ETLStats:
    def __init__(self):
        self.errors = []
        self.start_time = timezone.now()
    
    def add_error(self, error):
        self.errors.append(f"{timezone.now()}: {error}")
```

**Real time Logs**:
![Screenshot 2025-06-06 223525](https://github.com/user-attachments/assets/55966319-4aa3-41dc-acdb-6263f195c0e7)

### 3. Database Auditing:
```
class Animal(models.Model):
    friends_raw = models.TextField()  # Original data
    friends = models.JSONField()      # Transformed data
    processed_at = models.DateTimeField(auto_now_add=True)
```

### 4. Standalone Script:
You can run the standalone script for ETL animal data as follows:
```
python animal_etl.py
```

### 5. Job Tracking:
```
class ETLProcessingLog(models.Model):
    status = models.CharField(choices=[
        ('running', 'Running'),
        ('completed', 'Completed')
    ])
    total_animals_fetched = models.IntegerField()
```

# Execution Options for ETL Animals Data
**Method 1: Django management Command**
```
python manage.py etl-service
```
Best For **Production runs**

**Method 2: Web Trigger**	
```
POST /etl/run
```
Best for **Manual execution**

**Method 3: Standalone Python Script**	
```
python animal_etl.py
```
Best for **Simple deployments**
