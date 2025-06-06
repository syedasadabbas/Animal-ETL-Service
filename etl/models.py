from django.db import models

class Animal(models.Model):
    """
    Model to store animal data with original and transformed fields
    """
    api_id = models.IntegerField(unique=True, help_text="Original ID from API")
    name = models.CharField(max_length=255)
    species = models.CharField(max_length=100)
    age = models.IntegerField(null=True, blank=True)
    
    friends_raw = models.TextField(help_text="Original comma-delimited friends string")
    born_at_raw = models.CharField(max_length=100, null=True, blank=True, help_text="Original born_at string")
    
    friends = models.JSONField(default=list, help_text="Transformed friends array")
    born_at = models.DateTimeField(null=True, blank=True, help_text="Transformed ISO8601 UTC timestamp")
    
    processed_at = models.DateTimeField(auto_now_add=True)
    is_processed = models.BooleanField(default=False)
    is_sent_to_home = models.BooleanField(default=False)
    
    class Meta:
        db_table = 'animals'
        ordering = ['api_id']
        
    def __str__(self):
        return f"{self.name} ({self.species}) - ID: {self.api_id}"
    
    def to_home_format(self):
        """
        Convert the animal data to the format expected by /animals/v1/home endpoint
        """
        return {
            'id': self.api_id,
            'name': self.name,
            'species': self.species,
            'age': self.age,
            'friends': self.friends,
            'born_at': self.born_at.isoformat() if self.born_at else None
        }

class ETLProcessingLog(models.Model):
    """
    Model to track ETL processing statistics and logs
    """
    process_start = models.DateTimeField(auto_now_add=True)
    process_end = models.DateTimeField(null=True, blank=True)
    total_animals_fetched = models.IntegerField(default=0)
    total_animals_processed = models.IntegerField(default=0)
    total_animals_sent = models.IntegerField(default=0)
    batches_posted = models.IntegerField(null=True, blank=True, default=0)
    errors_encountered = models.TextField(blank=True)
    status = models.CharField(max_length=20, choices=[
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('partial', 'Partially Completed')
    ], default='running')
    
    class Meta:
        db_table = 'etl_processing_logs'
        ordering = ['-process_start']
        
    def __str__(self):
        return f"ETL Process {self.id} - {self.status} ({self.process_start})"

class APIErrorLog(models.Model):
    """
    Model to track API errors and retry attempts
    """
    endpoint = models.CharField(max_length=255)
    error_type = models.CharField(max_length=50)
    error_message = models.TextField()
    http_status_code = models.IntegerField(null=True, blank=True)
    retry_attempt = models.IntegerField(default=0)
    occurred_at = models.DateTimeField(auto_now_add=True)
    resolved = models.BooleanField(default=False)
    
    class Meta:
        db_table = 'api_error_logs'
        ordering = ['-occurred_at']
        
    def __str__(self):
        return f"API Error: {self.endpoint} - {self.error_type} (Attempt {self.retry_attempt})"