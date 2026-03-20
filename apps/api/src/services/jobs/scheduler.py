import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apps.api.src.services.jobs.feedback_emailer import send_daily_feedback_report
from apps.api.src.services.jobs.mis_emailer import send_daily_mis_report

logger = logging.getLogger(__name__)
scheduler = None

def start_scheduler():
    global scheduler
    if scheduler is not None: return
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    
    scheduler.add_job(send_daily_feedback_report, trigger=CronTrigger(hour=23, minute=30), id="daily_feedback_email")
    scheduler.add_job(send_daily_mis_report, trigger=CronTrigger(hour=23, minute=30), id="daily_mis_report")
    
    scheduler.start()
    logger.info("Job scheduler started")

def stop_scheduler():
    global scheduler
    if scheduler: scheduler.shutdown(); scheduler = None
