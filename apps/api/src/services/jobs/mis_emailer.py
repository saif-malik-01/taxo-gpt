import logging
from datetime import datetime, timedelta
from sqlalchemy import select, func
from apps.api.src.db.session import AsyncSessionLocal
from apps.api.src.db.models.base import User, ChatMessage, CreditLog, ChatSession
from apps.api.src.services.email import EmailService
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

RECIPIENT_EMAIL = settings.FEEDBACK_RECIPIENT_EMAIL

async def get_daily_mis_data():
    """Fetch usage and growth data for the last 24 hours."""
    async with AsyncSessionLocal() as session:
        yesterday = datetime.now() - timedelta(days=1)
        
        new_users = (await session.execute(select(func.count(User.id)).where(User.created_at >= yesterday))).scalar() or 0
        token_res = (await session.execute(select(func.sum(ChatMessage.prompt_tokens), func.sum(ChatMessage.response_tokens)).where(ChatMessage.timestamp >= yesterday))).first()
        total_tokens = (token_res[0] or 0) + (token_res[1] or 0)
        sessions_count = (await session.execute(select(func.count(ChatSession.id)).where(ChatSession.created_at >= yesterday))).scalar() or 0

        return {
            "new_users": new_users,
            "total_tokens": total_tokens,
            "sessions": sessions_count,
            "timestamp": datetime.now()
        }

def format_mis_html(data):
    html = f"""
    <html>
        <body style="background: #121212; color: #fff; font-family: sans-serif; padding: 20px;">
            <h1 style="color: #fb923c;">Daily MIS Report</h1>
            <p>Date: {data['timestamp'].strftime('%B %d, %Y')}</p>
            <ul>
                <li>New Users: {data['new_users']}</li>
                <li>Total Tokens: {data['total_tokens']:,}</li>
                <li>Sessions Created: {data['sessions']}</li>
            </ul>
        </body>
    </html>
    """
    return html

async def send_daily_mis_report():
    try:
        logger.info("Starting Daily MIS Report Job")
        data = await get_daily_mis_data()
        subject = f"Daily MIS Report - {datetime.now().strftime('%B %d, %Y')}"
        EmailService.send_email(subject, format_mis_html(data), RECIPIENT_EMAIL)
        logger.info("Daily MIS report sent")
    except Exception as e:
        logger.error(f"Error in MIS job: {e}")
