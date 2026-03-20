import logging
from datetime import datetime, timedelta
from sqlalchemy import select, func
from apps.api.src.db.session import AsyncSessionLocal
from apps.api.src.db.models.base import Feedback, ChatMessage, User, ChatSession
from apps.api.src.services.email import EmailService
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

RECIPIENT_EMAIL = settings.FEEDBACK_RECIPIENT_EMAIL

async def get_daily_feedback():
    """Fetch feedback created in the last 24 hours."""
    async with AsyncSessionLocal() as session:
        yesterday = datetime.now() - timedelta(days=1)
        
        query = (
            select(Feedback, ChatMessage, User)
            .join(ChatMessage, Feedback.message_id == ChatMessage.id)
            .join(ChatSession, ChatMessage.session_id == ChatSession.id)
            .join(User, ChatSession.user_id == User.id)
            .where(Feedback.created_at >= yesterday)
            .order_by(Feedback.created_at.desc())
        )
        
        result = await session.execute(query)
        feedback_data = result.all()
        
        return feedback_data

def format_feedback_html(feedback_data):
    """Format feedback data as HTML email."""
    if not feedback_data:
        html = f"""
        <html>
            <head>
                <style>
                    body {{ background-color: #0a0a0a; color: #ffffff; font-family: sans-serif; text-align: center; }}
                    .container {{ max-width: 600px; margin: 40px auto; padding: 40px; background-color: #121212; border: 1px solid #333; }}
                    .brand {{ color: #fb923c; font-size: 24px; font-weight: bold; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    <h1>Daily Feedback Report</h1>
                    <p>{datetime.now().strftime('%B %d, %Y')}</p>
                    <p>No feedback submissions received in the last 24 hours.</p>
                </div>
            </body>
        </html>
        """
        return html
    
    # Simple version for now (can expand if needed)
    feedback_items = []
    for f, msg, user in feedback_data:
        items = f"<div><b>{user.email}</b>: {f.rating} stars - {f.comment or ''} <br><i>Msg: {msg.content[:100]}...</i></div>"
        feedback_items.append(items)

    html = f"""
    <html>
        <body>
            <h1>Daily Feedback Report</h1>
            <p>{len(feedback_data)} submissions received.</p>
            <hr>
            {''.join(feedback_items)}
        </body>
    </html>
    """
    return html

async def send_daily_feedback_report():
    try:
        logger.info("Starting Daily Feedback Report Job")
        feedback_data = await get_daily_feedback()
        subject = f"Daily Feedback Report - {datetime.now().strftime('%B %d, %Y')}"
        html_content = format_feedback_html(feedback_data)
        EmailService.send_email(subject, html_content, RECIPIENT_EMAIL)
        logger.info("Daily feedback report processed")
    except Exception as e:
        logger.error(f"Error in feedback job: {e}")
