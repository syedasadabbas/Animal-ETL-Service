from django.contrib import admin

from .models import Animal, APIErrorLog, ETLProcessingLog


@admin.register(Animal)
class AnimalAdmin(admin.ModelAdmin):
    list_display = (
        "api_id",
        "name",
        "species",
        "age",
        "is_processed",
        "is_sent_to_home",
        "processed_at",
    )
    search_fields = ("name", "species", "api_id")
    list_filter = ("species", "is_processed", "is_sent_to_home")
    ordering = ("-processed_at",)


@admin.register(ETLProcessingLog)
class ETLProcessingLogAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "status",
        "process_start",
        "process_end",
        "total_animals_fetched",
        "total_animals_processed",
        "total_animals_sent",
    )
    ordering = ("-process_start",)


@admin.register(APIErrorLog)
class APIErrorLogAdmin(admin.ModelAdmin):
    list_display = (
        "endpoint",
        "error_type",
        "http_status_code",
        "retry_attempt",
        "occurred_at",
        "resolved",
    )
    search_fields = ("endpoint", "error_type", "error_message")
    list_filter = ("resolved", "error_type", "http_status_code")
    ordering = ("-occurred_at",)
