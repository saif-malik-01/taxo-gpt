import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from api.config import settings

from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders

logger = logging.getLogger(__name__)

class EmailService:
    @staticmethod
    def send_email(subject: str, html_content: str, recipient: str, attachments: list[dict] = None):
        """
        Send email using SMTP configuration.
        attachments: list of dicts like {"filename": "invoice.pdf", "content": b"..."}
        """
        
        if not settings.SMTP_USERNAME or not settings.SMTP_PASSWORD:
            logger.error("SMTP credentials not configured. Skipping email.")
            return False
        
        try:
            # Create message
            message = MIMEMultipart("mixed")
            message["Subject"] = subject
            message["From"] = settings.SMTP_USERNAME
            message["To"] = recipient
            
            # Attach HTML content
            content_part = MIMEMultipart("alternative")
            html_part = MIMEText(html_content, "html")
            content_part.attach(html_part)
            message.attach(content_part)

            # Add attachments
            if attachments:
                for att in attachments:
                    part = MIMEApplication(att["content"], _subtype="pdf")
                    part.add_header('Content-Disposition', 'attachment', filename=att["filename"])
                    message.attach(part)
            
            # Send email
            logger.info(f"Connecting to {settings.SMTP_SERVER}:{settings.SMTP_PORT}...")
            with smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT) as server:
                server.starttls()
                server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
                server.send_message(message)
            
            logger.info(f"Email sent successfully to {recipient}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email to {recipient}: {e}")
            return False

    @staticmethod
    def send_verification_email(email: str, token: str, full_name: str = None):
        """Send a verification email to a new user."""
        verification_url = f"{settings.FRONTEND_URL}/verify-email?token={token}"
        
        subject = "Verify Your TaxoBuddy Account"
        
        name_greeting = f"Hi {full_name}," if full_name else "Hi there,"
        
        html_content = f"""
        <html>
            <head>
                <style>
                    @import url('https://fonts.googleapis.com/css2?family=Geist:wght@400;600&display=swap');
                    body {{ 
                        background-color: #0a0a0a; 
                        margin: 0; 
                        padding: 0; 
                        font-family: 'Geist', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                        color: #ffffff;
                    }}
                    .container {{ 
                        max-width: 600px; 
                        margin: 40px auto; 
                        padding: 40px; 
                        background-color: #121212; 
                        border: 1px solid rgba(251, 146, 60, 0.2); 
                        border-radius: 16px; 
                    }}
                    .brand {{
                        color: #fb923c;
                        font-size: 24px;
                        font-weight: 600;
                        letter-spacing: -0.025em;
                        margin-bottom: 32px;
                        text-align: center;
                    }}
                    .greeting {{
                        font-size: 20px;
                        margin-bottom: 16px;
                        color: #ffffff;
                    }}
                    .text {{
                        font-size: 16px;
                        line-height: 1.6;
                        color: #a1a1aa;
                        margin-bottom: 32px;
                    }}
                    .button-container {{
                        text-align: center;
                        margin: 40px 0;
                    }}
                    .button {{
                        background-color: #ffffff;
                        color: #000000 !important;
                        padding: 14px 32px;
                        text-decoration: none;
                        border-radius: 8px;
                        font-weight: 600;
                        font-size: 16px;
                        display: inline-block;
                        transition: transform 0.2s ease;
                    }}
                    .footer {{
                        margin-top: 48px;
                        padding-top: 24px;
                        border-top: 1px solid rgba(255, 255, 255, 0.05);
                        text-align: center;
                        font-size: 12px;
                        color: #52525b;
                        letter-spacing: 0.05em;
                        text-transform: uppercase;
                    }}
                    .link-fallback {{
                        word-break: break-all;
                        font-size: 13px;
                        color: #3f3f46;
                        margin-top: 24px;
                        text-align: center;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    
                    <div class="greeting">{name_greeting}</div>
                    
                    <p class="text">
                        Welcome to the future of tax compliance. To activate your account and start your journey with TaxoBuddy, please verify your email address.
                    </p>
                    
                    <div class="button-container">
                        <a href="{verification_url}" class="button">Verify Account</a>
                    </div>
                    
                    <p class="text" style="text-align: center; margin-bottom: 0;">
                        Ready to automate your professional workflow?
                    </p>
                    
                    <div class="link-fallback">
                        If the button doesn't work, copy this link: <br/>
                        <a href="{verification_url}" style="color: #fb923c; text-decoration: none;">{verification_url}</a>
                    </div>
                    
                    <div class="footer">
                        Automated Message • TaxoBuddy Intelligence
                    </div>
                </div>
            </body>
        </html>
        """
        
        return EmailService.send_email(subject, html_content, email)

    @staticmethod
    def send_invoice_email(email: str, invoice_pdf: bytes, order_id: str, amount: float, full_name: str = None):
        """Send a payment confirmation and invoice to the user."""
        subject = f"Invoice for your TaxoBuddy Purchase - {order_id}"
        
        name_greeting = f"Hi {full_name}," if full_name else "Hi there,"
        
        # Convert paise/cents to standard currency unit
        display_amount = amount / 100
        
        html_content = f"""
        <html>
            <head>
                <style>
                    @import url('https://fonts.googleapis.com/css2?family=Geist:wght@400;600&display=swap');
                    body {{ 
                        background-color: #0a0a0a; 
                        margin: 0; 
                        padding: 0; 
                        font-family: 'Geist', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                        color: #ffffff;
                    }}
                    .container {{ 
                        max-width: 600px; 
                        margin: 40px auto; 
                        padding: 40px; 
                        background-color: #121212; 
                        border: 1px solid rgba(251, 146, 60, 0.2); 
                        border-radius: 16px; 
                    }}
                    .brand {{
                        color: #fb923c;
                        font-size: 24px;
                        font-weight: 600;
                        letter-spacing: -0.025em;
                        margin-bottom: 32px;
                        text-align: center;
                    }}
                    .greeting {{
                        font-size: 20px;
                        margin-bottom: 16px;
                        color: #ffffff;
                    }}
                    .text {{
                        font-size: 16px;
                        line-height: 1.6;
                        color: #a1a1aa;
                        margin-bottom: 24px;
                    }}
                    .receipt-card {{
                        background-color: rgba(255, 255, 255, 0.03);
                        border: 1px solid rgba(255, 255, 255, 0.05);
                        border-radius: 12px;
                        padding: 24px;
                        margin-bottom: 32px;
                    }}
                    .detail-row {{
                        display: flex;
                        justify-content: space-between;
                        margin-bottom: 12px;
                        font-size: 14px;
                    }}
                    .footer {{
                        margin-top: 48px;
                        padding-top: 24px;
                        border-top: 1px solid rgba(255, 255, 255, 0.05);
                        text-align: center;
                        font-size: 12px;
                        color: #52525b;
                        letter-spacing: 0.05em;
                        text-transform: uppercase;
                    }}
                    .detail-row span {{
                        display: inline-block;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    
                    <div class="greeting">{name_greeting}</div>
                    
                    <p class="text">
                        Thank you for your purchase. Your payment was successful and your account has been updated with your new credits.
                    </p>

                    <div class="receipt-card">
                        <div style="font-weight: 600; margin-bottom: 16px; color: #fb923c; text-transform: uppercase; font-size: 12px; letter-spacing: 0.1em;">Order Summary</div>
                        <div class="detail-row">
                            <span style="color: #52525b;">Order ID:</span>
                            <span>{order_id}</span>
                        </div>
                        <div class="detail-row">
                            <span style="color: #52525b;">Amount Paid:</span>
                            <span style="font-weight: 600; color: #ffffff;">INR {display_amount:.2f}</span>
                        </div>
                    </div>
                    
                    <p class="text">
                        We have attached the official invoice to this email for your records. 
                        If you have any questions about this charge, please contact our support team.
                    </p>

                    <div style="text-align: center; margin: 40px 0;">
                        <a href="{settings.FRONTEND_URL}/dashboard" style="background-color: #ffffff; color: #000000; padding: 14px 32px; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 16px; display: inline-block;">Go to Dashboard</a>
                    </div>
                    
                    <div class="footer">
                        Secure Payment Notification • TaxoBuddy Intelligence
                    </div>
                </div>
            </body>
        </html>
        """
        
        attachments = [
            {
                "filename": f"Invoice-{order_id}.pdf",
                "content": invoice_pdf
            }
        ]
        
        return EmailService.send_email(subject, html_content, email, attachments=attachments)

    @staticmethod
    def send_low_credit_notification(email: str, credit_type: str, remaining_balance: int, full_name: str = None):
        """Send a notification when user credits are running low."""
        subject = f"Alert: Low {credit_type.title()} Credits on TaxoBuddy"
        
        name_greeting = f"Hi {full_name}," if full_name else "Hi there,"
        
        credit_display = "Draft Reply" if credit_type == "draft" else "Simple Query"
        
        html_content = f"""
        <html>
            <head>
                <style>
                    @import url('https://fonts.googleapis.com/css2?family=Geist:wght@400;600&display=swap');
                    body {{ 
                        background-color: #0a0a0a; 
                        margin: 0; 
                        padding: 0; 
                        font-family: 'Geist', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                        color: #ffffff;
                    }}
                    .container {{ 
                        max-width: 600px; 
                        margin: 40px auto; 
                        padding: 40px; 
                        background-color: #121212; 
                        border: 1px solid rgba(251, 146, 60, 0.2); 
                        border-radius: 16px; 
                    }}
                    .brand {{
                        color: #fb923c;
                        font-size: 24px;
                        font-weight: 600;
                        letter-spacing: -0.025em;
                        margin-bottom: 32px;
                        text-align: center;
                    }}
                    .alert-badge {{
                        background-color: rgba(251, 146, 60, 0.1);
                        color: #fb923c;
                        padding: 8px 16px;
                        border-radius: 99px;
                        font-size: 12px;
                        font-weight: 600;
                        text-transform: uppercase;
                        letter-spacing: 0.1em;
                        display: inline-block;
                        margin-bottom: 24px;
                    }}
                    .greeting {{
                        font-size: 20px;
                        margin-bottom: 16px;
                        color: #ffffff;
                    }}
                    .text {{
                        font-size: 16px;
                        line-height: 1.6;
                        color: #a1a1aa;
                        margin-bottom: 32px;
                    }}
                    .credits-card {{
                        background-color: rgba(255, 255, 255, 0.03);
                        border: 1px solid rgba(255, 255, 255, 0.05);
                        border-radius: 12px;
                        padding: 32px;
                        text-align: center;
                        margin-bottom: 40px;
                    }}
                    .count {{
                        font-size: 48px;
                        font-weight: 600;
                        color: #fb923c;
                        line-height: 1;
                        margin-bottom: 8px;
                    }}
                    .label {{
                        color: #52525b;
                        font-size: 14px;
                        text-transform: uppercase;
                        letter-spacing: 0.05em;
                    }}
                    .button-container {{
                        text-align: center;
                    }}
                    .button {{
                        background-color: #ffffff;
                        color: #000000 !important;
                        padding: 14px 32px;
                        text-decoration: none;
                        border-radius: 8px;
                        font-weight: 600;
                        font-size: 16px;
                        display: inline-block;
                    }}
                    .footer {{
                        margin-top: 48px;
                        padding-top: 24px;
                        border-top: 1px solid rgba(255, 255, 255, 0.05);
                        text-align: center;
                        font-size: 12px;
                        color: #3f3f46;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    
                    <div style="text-align: center;">
                        <div class="alert-badge">Action Required</div>
                    </div>
                    
                    <div class="greeting">{name_greeting}</div>
                    
                    <p class="text">
                        Your <strong>{credit_display}</strong> credits are almost exhausted. To ensure uninterrupted service for your tax professional workflow, please top up your account.
                    </p>

                    <div class="credits-card">
                        <div class="count">{remaining_balance}</div>
                        <div class="label">Credit Remaining</div>
                    </div>
                    
                    <div class="button-container">
                        <a href="{settings.FRONTEND_URL}/pricing" class="button">Buy More Credits</a>
                    </div>
                    
                    <div class="footer">
                        Automated System Notification • TaxoBuddy Intelligence
                    </div>
                </div>
            </body>
        </html>
        """
        
        return EmailService.send_email(subject, html_content, email)
