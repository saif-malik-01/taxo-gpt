"""
Job Scheduler for background tasks.
Uses APScheduler to run periodic jobs within the FastAPI application.
"""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from services.jobs.feedback_emailer import send_daily_feedback_report

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = None


def start_scheduler():
    """Initialize and start the job scheduler."""
    global scheduler
    
    if scheduler is not None:
        logger.warning("Scheduler already started")
        return
    
    logger.info("Initializing job scheduler...")
    
    # Create scheduler with timezone set to IST
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    
    # Add daily feedback email job - runs at 11:30 PM IST every day
    scheduler.add_job(
        send_daily_feedback_report,
        trigger=CronTrigger(hour=23, minute=30),  # 11:30 PM IST
        id="daily_feedback_email",
        name="Daily Feedback Email Report",
        replace_existing=True
    )
    
    logger.info("✅ Scheduled: Daily Feedback Email - 11:30 PM IST")
    
    # Start the scheduler
    scheduler.start()
    logger.info("✅ Job scheduler started successfully")


def stop_scheduler():
    """Stop the job scheduler."""
    global scheduler
    
    if scheduler is not None:
        logger.info("Stopping job scheduler...")
        scheduler.shutdown()
        scheduler = None
        logger.info("Job scheduler stopped")


def list_jobs():
    """List all scheduled jobs."""
    if scheduler is None:
        return []
    
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger)
        })
    return jobs
