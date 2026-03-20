import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from apps.api.src.core.config import settings

logger = logging.getLogger(__name__)

class EmailService:
    @staticmethod
    def send_email(subject: str, html_content: str, recipient: str, attachments: list[dict] = None):
        if not settings.SMTP_USERNAME or not settings.SMTP_PASSWORD:
            logger.error(f"SMTP not configured for {recipient}. Skipping.")
            return False
        
        try:
            message = MIMEMultipart("mixed")
            message["Subject"] = subject
            message["From"] = settings.SMTP_USERNAME
            message["To"] = recipient
            
            content_part = MIMEMultipart("alternative")
            content_part.attach(MIMEText(html_content, "html"))
            message.attach(content_part)

            if attachments:
                for att in attachments:
                    part = MIMEApplication(att["content"], _subtype="pdf")
                    part.add_header('Content-Disposition', 'attachment', filename=att["filename"])
                    message.attach(part)
            
            with smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT) as server:
                server.starttls()
                server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
                server.send_message(message)
            logger.info(f"✅ Email sent to {recipient}")
            return True
        except Exception as e:
            logger.error(f"❌ Email error for {recipient}: {e}")
            return False

    @staticmethod
    def send_verification_email(email: str, token: str, full_name: str = None):
        verification_url = f"{settings.FRONTEND_URL}/verify-email?token={token}"
        subject = "Verify Your TaxoBuddy Account"
        name_greeting = f"Hi {full_name}," if full_name else "Hi there,"
        
        html_content = f"""
        <html>
            <head>
                <style>
                    body {{ background-color: #0a0a0a; color: #ffffff; font-family: sans-serif; padding: 40px; }}
                    .container {{ max-width: 600px; margin: auto; background: #121212; padding: 40px; border-radius: 16px; border: 1px solid #333; }}
                    .button {{ background: #ffffff; color: #000000; padding: 12px 24px; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block; margin: 20px 0; }}
                    .brand {{ color: #fb923c; font-size: 24px; font-weight: bold; margin-bottom: 20px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    <div>{name_greeting}</div>
                    <p>Welcome to TaxoBuddy. Please verify your email to start.</p>
                    <a href="{verification_url}" class="button">Verify Account</a>
                    <p>Or copy this link: {verification_url}</p>
                </div>
            </body>
        </html>
        """
        return EmailService.send_email(subject, html_content, email)

    @staticmethod
    def send_invoice_email(email: str, invoice_pdf: bytes, order_id: str, amount: float, full_name: str = None):
        # (Restore invoice template from backup if needed)
        pass
