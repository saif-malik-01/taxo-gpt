import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, or_, and_
from apps.api.src.db.session import AsyncSessionLocal
from apps.api.src.db.models.base import User
from apps.api.src.services.email import EmailService

logger = logging.getLogger(__name__)

async def send_reengagement_emails():
    """
    Finds users who haven't logged in for 15+ days and haven't 
    received a re-engagement email since their last activity.
    """
    try:
        logger.info("Starting Re-engagement Email Job")
        async with AsyncSessionLocal() as db:
            now = datetime.now(timezone.utc)
            fifteen_days_ago = now - timedelta(days=15)
            
            # Find users who:
            # 1. Last logged in more than 15 days ago
            # 2. Haven't been sent a re-engagement email recently 
            #    (either never sent, or sent before their last login)
            query = (
                select(User)
                .where(
                    and_(
                        User.last_login_at <= fifteen_days_ago,
                        or_(
                            User.reengagement_email_sent_at == None,
                            User.reengagement_email_sent_at < User.last_login_at
                        )
                    )
                )
            )
            
            result = await db.execute(query)
            users_to_notify = result.scalars().all()
            
            logger.info(f"Found {len(users_to_notify)} users for re-engagement")
            
            count = 0
            for user in users_to_notify:
                try:
                    success = EmailService.send_reengagement_email(
                        email=user.email,
                        full_name=user.full_name
                    )
                    if success:
                        user.reengagement_email_sent_at = now
                        count += 1
                except Exception as e:
                    logger.error(f"Failed to send re-engagement email to {user.email}: {e}")
            
            if count > 0:
                await db.commit()
                logger.info(f"Successfully sent {count} re-engagement emails")
            else:
                logger.info("No re-engagement emails needed to be sent today")
                
    except Exception as e:
        logger.error(f"Error in re-engagement job: {e}")
