# Background Jobs Module

This module handles scheduled background jobs using APScheduler.

## Jobs

### 1. Daily Feedback Email Report

- **Schedule**: Daily at 11:30 PM IST
- **Function**: `services.jobs.feedback_emailer.send_daily_feedback_report()`
- **Purpose**: Sends an HTML email report with all feedback from the last 24 hours to configured recipient
- **Recipient**: Configured via `FEEDBACK_RECIPIENT_EMAIL` env variable
- **Email Format**: Professional HTML with statistics and detailed feedback entries

## Configuration

All jobs are configured in `scheduler.py`. The scheduler:
- Starts automatically when the FastAPI server starts
- Stops automatically when the server shuts down
- Uses IST timezone (Asia/Kolkata)
- Logs all job execution to the server logs

## Adding New Jobs

To add a new scheduled job:

1. Create your job function in `services/jobs/`
2. Import it in `scheduler.py`
3. Add it to the scheduler in `start_scheduler()`:

```python
scheduler.add_job(
    your_job_function,
    trigger=CronTrigger(hour=10, minute=0),  # Daily at 10 AM
    id="your_job_id",
    name="Your Job Name",
    replace_existing=True
)
```

## Testing Jobs

Use the admin API endpoints:

- `GET /admin/jobs` - List all scheduled jobs
- `POST /admin/jobs/feedback/trigger` - Manually trigger feedback report

## Monitoring

All job execution is logged. Check your server logs:

```bash
sudo journalctl -u taxogpt -f | grep -i "job"
```
