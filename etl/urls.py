from django.urls import path

from . import views

app_name = "etl"

urlpatterns = [
    path("", views.etl_dashboard, name="etl_dashboard"),
    path("status/", views.etl_status, name="status"),
    path("run/", views.ETLRunView.as_view(), name="run-etl"),
    path("run-tests/", views.run_etl_tests, name="run_etl_tests"),
]
