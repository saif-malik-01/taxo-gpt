"""
Feedback Email Service - Internal Job
Sends daily feedback reports via email.
This runs as part of the FastAPI server using APScheduler.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import os
import logging

from sqlalchemy import select
from services.database import AsyncSessionLocal
from services.models import Feedback, ChatMessage, User, ChatSession

logger = logging.getLogger(__name__)


# Email Configuration from environment variables
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
RECIPIENT_EMAIL = os.getenv("FEEDBACK_RECIPIENT_EMAIL", "atul@gmail.com")


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
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                    .header {{ background-color: #4CAF50; color: white; padding: 20px; text-align: center; }}
                    .content {{ padding: 20px; }}
                    .no-feedback {{ text-align: center; color: #666; padding: 40px; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>📊 Daily Feedback Report</h1>
                    <p>{datetime.now().strftime('%B %d, %Y')}</p>
                </div>
                <div class="content">
                    <div class="no-feedback">
                        <h2>No feedback received today</h2>
                        <p>There were no feedback submissions in the last 24 hours.</p>
                    </div>
                </div>
            </body>
        </html>
        """
        return html
    
    # Build feedback items
    feedback_items = []
    for feedback, message, user in feedback_data:
        rating_stars = "⭐" * feedback.rating if feedback.rating > 0 else f"👎 ({feedback.rating})"
        
        item_html = f"""
        <div class="feedback-item">
            <div class="feedback-header">
                <span class="rating">{rating_stars}</span>
                <span class="date">{feedback.created_at.strftime('%Y-%m-%d %H:%M:%S IST')}</span>
            </div>
            <div class="user-info">
                <strong>User:</strong> {user.email}
            </div>
            <div class="message-preview">
                <strong>Message:</strong>
                <p>{message.content[:200]}{'...' if len(message.content) > 200 else ''}</p>
            </div>
            {f'<div class="comment"><strong>Comment:</strong> <p>{feedback.comment}</p></div>' if feedback.comment else ''}
        </div>
        """
        feedback_items.append(item_html)
    
    total_feedback = len(feedback_data)
    avg_rating = sum(f.rating for f, _, _ in feedback_data) / total_feedback if total_feedback > 0 else 0
    
    html = f"""
    <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .header {{ background-color: #4CAF50; color: white; padding: 20px; text-align: center; }}
                .stats {{ background-color: #f4f4f4; padding: 15px; margin: 20px 0; border-radius: 5px; }}
                .stats-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; }}
                .stat-item {{ text-align: center; }}
                .stat-value {{ font-size: 24px; font-weight: bold; color: #4CAF50; }}
                .stat-label {{ color: #666; font-size: 14px; }}
                .content {{ padding: 20px; }}
                .feedback-item {{ 
                    border: 1px solid #ddd; 
                    border-radius: 5px; 
                    padding: 15px; 
                    margin-bottom: 15px;
                    background-color: #fff;
                }}
                .feedback-header {{ 
                    display: flex; 
                    justify-content: space-between; 
                    margin-bottom: 10px;
                    border-bottom: 1px solid #eee;
                    padding-bottom: 10px;
                }}
                .rating {{ font-size: 18px; }}
                .date {{ color: #666; font-size: 14px; }}
                .user-info {{ margin-bottom: 10px; color: #555; }}
                .message-preview {{ 
                    background-color: #f9f9f9; 
                    padding: 10px; 
                    border-left: 3px solid #4CAF50;
                    margin: 10px 0;
                }}
                .comment {{ 
                    background-color: #fff3cd; 
                    padding: 10px; 
                    border-left: 3px solid #ffc107;
                    margin: 10px 0;
                }}
                .footer {{ 
                    text-align: center; 
                    padding: 20px; 
                    color: #666; 
                    font-size: 12px;
                    border-top: 1px solid #ddd;
                    margin-top: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Daily Feedback Report</h1>
                <p>{datetime.now().strftime('%B %d, %Y')}</p>
            </div>
            
            <div class="content">
                <div class="stats">
                    <h2>Summary</h2>
                    <div class="stats-grid">
                        <div class="stat-item">
                            <div class="stat-value">{total_feedback}</div>
                            <div class="stat-label">Total Feedback</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">{avg_rating:.1f} / 5</div>
                            <div class="stat-label">Average Rating</div>
                        </div>
                    </div>
                </div>
                
                <h2>Feedback Details</h2>
                {''.join(feedback_items)}
            </div>
            
            <div class="footer">
                <p>This is an automated report generated by the GST Expert API system.</p>
                <p>Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}</p>
            </div>
        </body>
    </html>
    """
    
    return html


def send_email(subject, html_content, recipient):
    """Send email using SMTP."""
    
    if not SMTP_USERNAME or not SMTP_PASSWORD:
        logger.error("SMTP credentials not configured. Skipping email.")
        return False
    
    try:
        # Create message
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = SMTP_USERNAME
        message["To"] = recipient
        
        # Attach HTML content
        html_part = MIMEText(html_content, "html")
        message.attach(html_part)
        
        # Send email
        logger.info(f"Connecting to {SMTP_SERVER}:{SMTP_PORT}...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(message)
        
        logger.info(f"Email sent successfully to {recipient}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


async def send_daily_feedback_report():
    """
    Main job function that sends daily feedback report.
    This is called by the scheduler.
    """
    try:
        logger.info("=" * 60)
        logger.info("Starting Daily Feedback Report Job")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
        logger.info("=" * 60)
        
        # Fetch today's feedback
        logger.info("Fetching feedback from database...")
        feedback_data = await get_daily_feedback()
        
        logger.info(f"Found {len(feedback_data)} feedback entries in the last 24 hours")
        
        # Format email
        subject = f"Daily Feedback Report - {datetime.now().strftime('%B %d, %Y')}"
        html_content = format_feedback_html(feedback_data)
        
        # Send email
        logger.info(f"Sending email to {RECIPIENT_EMAIL}...")
        success = send_email(subject, html_content, RECIPIENT_EMAIL)
        
        if success:
            logger.info("Daily feedback report sent successfully!")
        else:
            logger.warning("Failed to send daily feedback report")
        
        logger.info("=" * 60)
        logger.info("Job completed")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in feedback report job: {e}", exc_info=True)
