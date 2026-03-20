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
from services.email import EmailService
from api.config import settings

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
                    body {{ 
                        background-color: #0a0a0a; 
                        margin: 0; 
                        padding: 0; 
                        font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                        color: #ffffff;
                    }}
                    .container {{ 
                        max-width: 600px; 
                        margin: 40px auto; 
                        padding: 40px; 
                        background-color: #121212; 
                        border: 1px solid rgba(251, 146, 60, 0.2); 
                        border-radius: 16px; 
                        text-align: center;
                    }}
                    .brand {{ color: #fb923c; font-size: 24px; font-weight: 600; margin-bottom: 32px; }}
                    .no-feedback {{ color: #a1a1aa; padding: 40px; font-size: 18px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    <h1>Daily Feedback Report</h1>
                    <p style="color: #52525b;">{datetime.now().strftime('%B %d, %Y')}</p>
                    <div class="no-feedback">
                        No feedback submissions received in the last 24 hours.
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
                <span style="color: #fb923c;">User:</span> {user.email}
            </div>
            <div class="message-preview">
                <div style="color: #a1a1aa; font-size: 12px; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.05em;">Message</div>
                <p style="margin: 0;">{message.content[:200]}{'...' if len(message.content) > 200 else ''}</p>
            </div>
            {f'<div class="comment"><div style="color: #fb923c; font-size: 12px; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.05em;">Comment</div><p style="margin: 0;">{feedback.comment}</p></div>' if feedback.comment else ''}
        </div>
        """
        feedback_items.append(item_html)
    
    total_feedback = len(feedback_data)
    avg_rating = sum(f.rating for f, _, _ in feedback_data) / total_feedback if total_feedback > 0 else 0
    
    html = f"""
    <html>
        <head>
            <style>
                body {{ 
                    background-color: #0a0a0a; 
                    margin: 0; 
                    padding: 0; 
                    font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                    color: #ffffff;
                }}
                .container {{ 
                    max-width: 800px; 
                    margin: 40px auto; 
                    padding: 40px; 
                    background-color: #121212; 
                    border: 1px solid rgba(251, 146, 60, 0.1); 
                    border-radius: 20px; 
                }}
                .brand {{ color: #fb923c; font-size: 24px; font-weight: 600; margin-bottom: 8px; text-align: center; }}
                h1 {{ text-align: center; margin-top: 0; font-weight: 600; letter-spacing: -0.025em; }}
                .report-date {{ text-align: center; color: #52525b; margin-bottom: 40px; font-size: 14px; text-transform: uppercase; letter-spacing: 0.1em; }}
                
                .stats {{ 
                    display: table; 
                    width: 100%; 
                    margin-bottom: 40px; 
                    border-collapse: separate; 
                    border-spacing: 15px 0;
                }}
                .stat-card {{ 
                    display: table-cell; 
                    background-color: rgba(255, 255, 255, 0.03); 
                    border: 1px solid rgba(255, 255, 255, 0.05);
                    padding: 24px; 
                    border-radius: 12px; 
                    text-align: center;
                    width: 50%;
                }}
                .stat-value {{ font-size: 32px; font-weight: 600; color: #fb923c; line-height: 1; }}
                .stat-label {{ color: #a1a1aa; font-size: 13px; margin-top: 8px; text-transform: uppercase; letter-spacing: 0.05em; }}
                
                .feedback-item {{ 
                    background-color: rgba(255, 255, 255, 0.02);
                    border: 1px solid rgba(255, 255, 255, 0.05); 
                    border-radius: 12px; 
                    padding: 24px; 
                    margin-bottom: 24px;
                }}
                .feedback-header {{ 
                    display: block;
                    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                    padding-bottom: 16px;
                    margin-bottom: 16px;
                }}
                .rating {{ font-size: 18px; }}
                .date {{ float: right; color: #52525b; font-size: 13px; }}
                .user-info {{ margin-bottom: 16px; font-size: 14px; color: #e5e5e5; }}
                .message-preview {{ 
                    background-color: rgba(0, 0, 0, 0.2); 
                    padding: 16px; 
                    border-radius: 8px;
                    margin: 16px 0;
                    border-left: 3px solid #fb923c;
                    font-size: 14px;
                    color: #d4d4d8;
                }}
                .comment {{ 
                    background-color: rgba(251, 146, 60, 0.05); 
                    padding: 16px; 
                    border-radius: 8px;
                    margin: 16px 0;
                    border-left: 3px solid #fb923c;
                    font-size: 14px;
                    color: #ffffff;
                }}
                .footer {{ 
                    text-align: center; 
                    margin-top: 60px; 
                    color: #3f3f46; 
                    font-size: 12px;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="brand">TaxoBuddy</div>
                <h1>Daily Feedback Report</h1>
                <div class="report-date">{datetime.now().strftime('%B %d, %Y')}</div>
                
                <div class="stats">
                    <div class="stat-card">
                        <div class="stat-value">{total_feedback}</div>
                        <div class="stat-label">Total Submissions</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">{avg_rating:.1f} / 5</div>
                        <div class="stat-label">Average Rating</div>
                    </div>
                </div>
                
                <h2 style="font-size: 18px; color: #fb923c; margin-bottom: 24px; text-transform: uppercase; letter-spacing: 0.1em;">Recent Activity</h2>
                {''.join(feedback_items)}
                
                <div class="footer">
                    Automated Insight System • TaxoBuddy Intelligence
                </div>
            </div>
        </body>
    </html>
    """
    
    return html


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
        success = EmailService.send_email(subject, html_content, RECIPIENT_EMAIL)
        
        if success:
            logger.info("Daily feedback report sent successfully!")
        else:
            logger.warning("Failed to send daily feedback report")
        
        logger.info("=" * 60)
        logger.info("Job completed")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in feedback report job: {e}", exc_info=True)
