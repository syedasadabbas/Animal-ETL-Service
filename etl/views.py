import json
import logging
from threading import Thread
from django.core.paginator import Paginator
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils.decorators import method_decorator
from django.views import View
from django.utils import timezone
from .utils.etl_service import run_etl_process, etl_stats
from .models import ETLProcessingLog, Animal

logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class ETLRunView(View):
    """Start ETL process"""
    
    def post(self, request):
        try:
            print("ETLRunView received POST request")
            data = json.loads(request.body)
            batch_size = data.get('batch_size', 100)
            
            job = ETLProcessingLog.objects.create(
                status='running',
            )
            
            thread = Thread(target=self.run_etl_background, args=(job.id, batch_size))
            thread.start()
            
            return JsonResponse({
                'success': True,
                'job_id': job.id,
                'message': 'ETL process started'
            })
            
        except Exception as e:
            logger.error(f"Error starting ETL process: {e}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)
    
    def run_etl_background(self, job_id, batch_size):
        try:
            print("Running ETL BACKGROUND")
            job = ETLProcessingLog.objects.get(id=job_id)
            stats = run_etl_process(batch_size=batch_size)
            
            job.status = 'completed' if stats.current_step == 'Completed' else 'failed'
            job.process_end = timezone.now()
            job.total_animals_fetched = stats.total_animals_found
            job.total_animals_processed = stats.animals_processed
            job.total_animals_sent = stats.animals_posted
            job.batches_posted = stats.batches_posted
            job.errors_encountered = json.dumps(stats.errors) if stats.errors else ""
            job.save()
            
        except Exception as e:
            logger.error(f"Background ETL process error: {e}")
            try:
                job = ETLProcessingLog.objects.get(id=job_id)
                job.status = 'failed'
                job.process_end = timezone.now()
                job.errors_encountered = json.dumps([str(e)])
                job.save()
            except:
                pass

@require_http_methods(["GET"])
def etl_status(request):
    """Get current ETL status"""
    return JsonResponse({
        'step': etl_stats.current_step,
        'total_found': etl_stats.total_animals_found,
        'processed': etl_stats.animals_processed,
        'posted': etl_stats.animals_posted,
        'batches': etl_stats.batches_posted,
        'errors': len(etl_stats.errors),
        'duration': etl_stats.get_duration(),
    })

def etl_dashboard(request):
    print("Running ETL VIEW")
    animal_list = Animal.objects.order_by("-processed_at")
    paginator = Paginator(animal_list, 25)
    page = request.GET.get("page")
    animals = paginator.get_page(page)

    latest_log = ETLProcessingLog.objects.first()

    return render(request, "etl/etl_dashboard.html", {
        "animals": animals,
        "etl_log": latest_log,
    })