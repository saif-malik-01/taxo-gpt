"""
MIS Report Email Service - Internal Job
Sends daily Management Information System (MIS) reports to admin.
Includes usage stats, user growth, and credit consumption.
"""

import logging
from datetime import datetime, timedelta
from sqlalchemy import select, func
from services.database import AsyncSessionLocal
from services.models import User, ChatMessage, CreditLog, ChatSession
from services.email import EmailService
from api.config import settings

logger = logging.getLogger(__name__)

RECIPIENT_EMAIL = settings.FEEDBACK_RECIPIENT_EMAIL

async def get_daily_mis_data():
    """Fetch usage and growth data for the last 24 hours."""
    async with AsyncSessionLocal() as session:
        yesterday = datetime.now() - timedelta(days=1)
        
        # 1. New Users
        new_users_query = select(func.count(User.id)).where(User.created_at >= yesterday)
        new_users_count = (await session.execute(new_users_query)).scalar() or 0
        
        # 2. Token Usage (from ChatMessage)
        token_query = select(
            func.sum(ChatMessage.prompt_tokens).label("input"),
            func.sum(ChatMessage.response_tokens).label("output")
        ).where(ChatMessage.timestamp >= yesterday)
        token_res = (await session.execute(token_query)).first()
        input_tokens = token_res[0] or 0
        output_tokens = token_res[1] or 0
        total_tokens = input_tokens + output_tokens
        
        # 3. Credit Consumption (Debits only)
        credit_query = select(
            CreditLog.credit_type,
            func.sum(CreditLog.amount).label("total")
        ).where(
            CreditLog.created_at >= yesterday,
            CreditLog.transaction_type == "usage",
            CreditLog.amount < 0
        ).group_by(CreditLog.credit_type)
        
        credit_res = await session.execute(credit_query)
        credits_spent = {row[0]: abs(row[1]) for row in credit_res.all()}
        
        # 4. Session Activity
        session_query = select(func.count(ChatSession.id)).where(ChatSession.created_at >= yesterday)
        sessions_created = (await session.execute(session_query)).scalar() or 0

        # 5. Active Users (Users who sent at least one message today)
        active_users_query = select(func.count(func.distinct(ChatSession.user_id))).join(ChatMessage).where(ChatMessage.timestamp >= yesterday)
        active_users_count = (await session.execute(active_users_query)).scalar() or 0

        return {
            "new_users": new_users_count,
            "active_users": active_users_count,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "credits_spent": credits_spent,
            "sessions_created": sessions_created,
            "timestamp": datetime.now()
        }

def format_mis_html(data):
    """Format MIS data as a beautiful HTML email."""
    
    date_str = data["timestamp"].strftime('%B %d, %Y')
    
    # Credit breakdown
    simple_spent = data["credits_spent"].get("simple", 0)
    draft_spent = data["credits_spent"].get("draft", 0)
    
    html = f"""
    <html>
        <head>
            <style>
                body {{ 
                    background-color: #0a0a0a; 
                    margin: 0; 
                    padding: 0; 
                    font-family: 'Geist', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
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
                
                .grid {{ 
                    display: grid;
                    grid-template-columns: repeat(2, 1fr);
                    gap: 20px;
                    margin-bottom: 40px;
                }}
                
                .card {{ 
                    background-color: rgba(255, 255, 255, 0.03); 
                    border: 1px solid rgba(255, 255, 255, 0.05);
                    padding: 24px; 
                    border-radius: 12px; 
                    text-align: center;
                }}
                
                .val {{ font-size: 28px; font-weight: 600; color: #fb923c; }}
                .lab {{ color: #a1a1aa; font-size: 12px; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.05em; }}
                
                .section-title {{
                    font-size: 14px;
                    color: #fb923c;
                    margin: 32px 0 16px 0;
                    text-transform: uppercase;
                    letter-spacing: 0.1em;
                    border-bottom: 1px solid rgba(251, 146, 60, 0.2);
                    padding-bottom: 8px;
                }}
                
                .footer {{ 
                    text-align: center; 
                    margin-top: 60px; 
                    color: #3f3f46; 
                    font-size: 11px;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                }}
                
                table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
                td {{ padding: 12px; border-bottom: 1px solid rgba(255,255,255,0.03); font-size: 14px; }}
                .label-td {{ color: #a1a1aa; width: 60%; }}
                .value-td {{ text-align: right; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="brand">TaxoBuddy</div>
                <h1>Daily MIS Report</h1>
                <div class="report-date">{date_str}</div>
                
                <div class="grid">
                    <div class="card">
                        <div class="val">{data["new_users"]}</div>
                        <div class="lab">New Users</div>
                    </div>
                    <div class="card">
                        <div class="val">{data["active_users"]}</div>
                        <div class="lab">Active Users</div>
                    </div>
                    <div class="card">
                        <div class="val">{data["total_tokens"]:,}</div>
                        <div class="lab">Total Tokens</div>
                    </div>
                    <div class="card">
                        <div class="val">{data["sessions_created"]}</div>
                        <div class="lab">Total Sessions</div>
                    </div>
                </div>
                
                <div class="section-title">Token Breakdown</div>
                <table>
                    <tr>
                        <td class="label-td">Input Tokens (User)</td>
                        <td class="value-td">{data["input_tokens"]:,}</td>
                    </tr>
                    <tr>
                        <td class="label-td">Output Tokens (AI)</td>
                        <td class="value-td">{data["output_tokens"]:,}</td>
                    </tr>
                </table>

                <div class="section-title">Credit Consumption</div>
                <table>
                    <tr>
                        <td class="label-td">Simple Query Credits</td>
                        <td class="value-td">{simple_spent}</td>
                    </tr>
                    <tr>
                        <td class="label-td">Draft Reply Credits</td>
                        <td class="value-td">{draft_spent}</td>
                    </tr>
                </table>
                
                <div class="footer">
                    Automated MIS Service • TaxoBuddy Intelligence
                </div>
            </div>
        </body>
    </html>
    """
    return html

async def send_daily_mis_report():
    """Main job function to send MIS report."""
    try:
        logger.info("Starting Daily MIS Report Job")
        
        data = await get_daily_mis_data()
        
        subject = f"Daily MIS Report - {datetime.now().strftime('%B %d, %Y')}"
        html_content = format_mis_html(data)
        
        success = EmailService.send_email(subject, html_content, RECIPIENT_EMAIL)
        
        if success:
            logger.info("MIS report sent successfully!")
        else:
            logger.error("Failed to send MIS report")
            
    except Exception as e:
        logger.error(f"Error in MIS report job: {e}", exc_info=True)
