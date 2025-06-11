import logging
from django.core.management.base import BaseCommand
from etl.utils.etl_service import run_etl_process

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("etl_process.log")],
)


class Command(BaseCommand):
    help = "Run ETL process to fetch, transform and load animals"

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Number of animals to process in each batch (default: 100)",
        )
        parser.add_argument(
            "--verbose", action="store_true", help="Enable verbose logging"
        )

    def handle(self, *args, **options):
        if options["verbose"]:
            logging.getLogger().setLevel(logging.DEBUG)

        batch_size = options["batch_size"]

        self.stdout.write(
            self.style.SUCCESS(f"Starting ETL process with batch size: {batch_size}")
        )

        stats = run_etl_process(batch_size=batch_size)

        self.display_results(stats)

    def display_results(self, stats):
        """Display ETL process results"""
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.SUCCESS("ETL PROCESS SUMMARY"))
        self.stdout.write("=" * 60)

        self.stdout.write(f"Status: {stats.current_step}")
        self.stdout.write(f"Duration: {stats.get_duration():.2f} seconds")
        self.stdout.write(f"Animals Found: {stats.total_animals_found}")
        self.stdout.write(f"Animals Processed: {stats.animals_processed}")
        self.stdout.write(f"Animals Posted: {stats.animals_posted}")
        self.stdout.write(f"Batches Posted: {stats.batches_posted}")

        if stats.errors:
            self.stdout.write(f"\nErrors ({len(stats.errors)}):")
            for error in stats.errors[-10:]:  # Show last 10 errors
                self.stdout.write(self.style.ERROR(f"  - {error}"))
            if len(stats.errors) > 10:
                self.stdout.write(f"  ... and {len(stats.errors) - 10} more errors")

        if stats.current_step == "Completed":
            self.stdout.write(
                self.style.SUCCESS("\n✅ ETL process completed successfully!")
            )
        else:
            self.stdout.write(self.style.ERROR("\n❌ ETL process failed or incomplete"))
