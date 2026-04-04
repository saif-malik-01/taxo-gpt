import smtplib
import logging
from datetime import datetime
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
                    body {{ background-color: #f4f7fa; color: #1e293b; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 40px; margin: 0; }}
                    .container {{ max-width: 600px; margin: auto; background: #ffffff; padding: 40px; border-radius: 16px; border: 1px solid #e2e8f0; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }}
                    .brand {{ color: #fb923c; font-size: 28px; font-weight: bold; margin-bottom: 24px; text-align: center; }}
                    .content {{ line-height: 1.6; color: #334155; font-size: 16px; }}
                    .button {{ background: #fb923c; color: #ffffff !important; padding: 14px 28px; text-decoration: none; border-radius: 10px; font-weight: bold; display: inline-block; margin: 24px 0; text-align: center; }}
                    .footer {{ margin-top: 32px; font-size: 12px; color: #94a3b8; text-align: center; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    <div class="content">
                        <p>{name_greeting}</p>
                        <p>Welcome to TaxoBuddy! We're excited to have you on board. To get started with professional tax intelligence, please verify your account by clicking the button below.</p>
                        <div style="text-align: center;">
                            <a href="{verification_url}" class="button">Verify My Account</a>
                        </div>
                        <p style="font-size: 14px; color: #64748b;">If the button doesn't work, copy and paste this link into your browser:<br>{verification_url}</p>
                    </div>
                    <div class="footer">
                        © {datetime.now().year} TaxoBuddy. All rights reserved.
                    </div>
                </div>
            </body>
        </html>
        """
        return EmailService.send_email(subject, html_content, email)

    @staticmethod
    def send_password_reset_email(email: str, token: str, full_name: str = None):
        reset_url = f"{settings.FRONTEND_URL}/auth/reset-password?token={token}"
        subject = "Reset Your TaxoBuddy Password"
        name_greeting = f"Hi {full_name}," if full_name else "Hi there,"
        
        html_content = f"""
        <html>
            <head>
                <style>
                    body {{ background-color: #f4f7fa; color: #1e293b; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 40px; margin: 0; }}
                    .container {{ max-width: 600px; margin: auto; background: #ffffff; padding: 40px; border-radius: 16px; border: 1px solid #e2e8f0; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }}
                    .brand {{ color: #fb923c; font-size: 28px; font-weight: bold; margin-bottom: 24px; text-align: center; }}
                    .content {{ line-height: 1.6; color: #334155; font-size: 16px; }}
                    .button {{ background: #fb923c; color: #ffffff !important; padding: 14px 28px; text-decoration: none; border-radius: 10px; font-weight: bold; display: inline-block; margin: 24px 0; text-align: center; }}
                    .footer {{ margin-top: 32px; font-size: 12px; color: #94a3b8; text-align: center; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    <div class="content">
                        <p>{name_greeting}</p>
                        <p>We received a request to reset the password for your TaxoBuddy account. If you made this request, please click the button below to set a new password. This link will expire in 1 hour.</p>
                        <div style="text-align: center;">
                            <a href="{reset_url}" class="button">Reset Password</a>
                        </div>
                        <p style="font-size: 14px; color: #64748b;">If you didn't request a password reset, you can safely ignore this email.</p>
                    </div>
                    <div class="footer">
                        © {datetime.now().year} TaxoBuddy. All rights reserved.
                    </div>
                </div>
            </body>
        </html>
        """
        return EmailService.send_email(subject, html_content, email)

    @staticmethod
    def send_invoice_email(email: str, invoice_pdf: bytes, order_id: str, amount: float, full_name: str = None):
        subject = f"Invoice for your TaxoBuddy Order {order_id}"
        amount_inr = amount / 100
        name_greeting = f"Hi {full_name}," if full_name else "Hi there,"
        
        html_content = f"""
        <html>
            <head>
                <style>
                    body {{ background-color: #f8fafc; color: #1e293b; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 40px; margin: 0; }}
                    .container {{ max-width: 600px; margin: auto; background: #ffffff; padding: 40px; border-radius: 16px; border: 1px solid #e2e8f0; box-shadow: 0 10px 15px rgba(0,0,0,0.05); }}
                    .brand {{ color: #fb923c; font-size: 28px; font-weight: bold; margin-bottom: 24px; text-align: center; }}
                    .content {{ line-height: 1.6; color: #334155; }}
                    .amount-box {{ background: #f1f5f9; padding: 24px; border-radius: 14px; margin: 24px 0; border: 1px solid #e2e8f0; text-align: center; }}
                    .amount-value {{ font-size: 28px; font-weight: bold; color: #fb923c; }}
                    .footer {{ color: #64748b; font-size: 13px; margin-top: 32px; text-align: center; padding-top: 24px; border-top: 1px solid #e2e8f0; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    <div class="content">
                        <p>{name_greeting}</p>
                        <p>Thank you for choosing TaxoBuddy! Your order <strong>{order_id}</strong> has been successfully processed.</p>
                        
                        <div class="amount-box">
                            <div style="font-size: 14px; color: #64748b; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.5px;">Total Amount Paid</div>
                            <div class="amount-value">INR {amount_inr:,.2f}</div>
                        </div>
                        
                        <p>We've attached your official tax invoice to this email for your records. Your purchased credits have been added to your account and are ready for use.</p>
                        <p>If you have any questions or need further assistance, please don't hesitate to reach out to our support team.</p>
                    </div>
                    <div class="footer">
                        © {datetime.now().year} TaxoBuddy Professional Tax Intelligence.<br>
                        This is an automated receipt for your recent transaction.
                    </div>
                </div>
            </body>
        </html>
        """
        
        attachments = [{
            "filename": f"TaxoBuddy_Invoice_{order_id}.pdf",
            "content": invoice_pdf
        }]
        
        return EmailService.send_email(subject, html_content, email, attachments)

    @staticmethod
    def send_low_credit_notification(email: str, balance: int, credit_type: str, full_name: str = None):
        subject = f"Low Credit Warning: {balance} {credit_type.capitalize()} Credits Left"
        name_greeting = f"Hi {full_name}," if full_name else "Hi there,"
        
        # Determine the purchase URL based on credit type if needed
        # For now, we'll just link to the main pricing/packages page
        pricing_url = f"{settings.FRONTEND_URL}/chat"

        html_content = f"""
        <html>
            <head>
                <style>
                    body {{ background-color: #f8fafc; color: #1e293b; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 40px; margin: 0; }}
                    .container {{ max-width: 600px; margin: auto; background: #ffffff; padding: 40px; border-radius: 16px; border: 1px solid #e2e8f0; box-shadow: 0 10px 15px rgba(0,0,0,0.05); }}
                    .brand {{ color: #fb923c; font-size: 28px; font-weight: bold; margin-bottom: 24px; text-align: center; }}
                    .content {{ line-height: 1.6; color: #334155; }}
                    .warning-box {{ background: #fff7ed; padding: 24px; border-radius: 14px; margin: 24px 0; border: 1px solid #fed7aa; text-align: center; }}
                    .balance-value {{ font-size: 32px; font-weight: bold; color: #ea580c; }}
                    .button {{ background: #fb923c; color: #ffffff !important; padding: 14px 28px; text-decoration: none; border-radius: 10px; font-weight: bold; display: inline-block; margin: 24px 0; text-align: center; }}
                    .footer {{ color: #64748b; font-size: 13px; margin-top: 32px; text-align: center; padding-top: 24px; border-top: 1px solid #e2e8f0; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    <div class="content">
                        <p>{name_greeting}</p>
                        <p>We're reaching out to let you know that your <strong>{credit_type.capitalize()}</strong> credit balance is running low.</p>
                        
                        <div class="warning-box">
                            <div style="font-size: 14px; color: #9a3412; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.5px;">Remaining Credits</div>
                            <div class="balance-value">{balance}</div>
                        </div>
                        
                        <p>To ensure uninterrupted access to TaxoBuddy's professional tax intelligence, we recommend topping up your account now.</p>
                        
                        <div style="text-align: center;">
                            <a href="{pricing_url}" class="button">Recharge Now</a>
                        </div>
                        
                        <p>If you recently made a purchase, please allow a few minutes for the credits to reflect in your account.</p>
                    </div>
                    <div class="footer">
                        © {datetime.now().year} TaxoBuddy Professional Tax Intelligence.<br>
                        This is an automated notification regarding your account balance.
                    </div>
                </div>
            </body>
        </html>
        """
        return EmailService.send_email(subject, html_content, email)

    @staticmethod
    def send_reengagement_email(email: str, full_name: str = None):
        subject = "We've missed you at TaxoBuddy!"
        name_greeting = f"Hi {full_name}," if full_name else "Hi there,"
        login_url = f"{settings.FRONTEND_URL}/auth/login"
        
        html_content = f"""
        <html>
            <head>
                <style>
                    body {{ background-color: #f8fafc; color: #1e293b; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 40px; margin: 0; }}
                    .container {{ max-width: 600px; margin: auto; background: #ffffff; padding: 40px; border-radius: 16px; border: 1px solid #e2e8f0; box-shadow: 0 10px 15px rgba(0,0,0,0.05); }}
                    .brand {{ color: #fb923c; font-size: 28px; font-weight: bold; margin-bottom: 24px; text-align: center; }}
                    .content {{ line-height: 1.6; color: #334155; font-size: 16px; }}
                    .feature-box {{ background: #f1f5f9; padding: 20px; border-radius: 12px; margin: 24px 0; border: 1px solid #e2e8f0; }}
                    .button {{ background: #fb923c; color: #ffffff !important; padding: 14px 28px; text-decoration: none; border-radius: 10px; font-weight: bold; display: inline-block; margin: 24px 0; text-align: center; }}
                    .footer {{ color: #64748b; font-size: 13px; margin-top: 32px; text-align: center; padding-top: 24px; border-top: 1px solid #e2e8f0; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="brand">TaxoBuddy</div>
                    <div class="content">
                        <p>{name_greeting}</p>
                        <p>It's been a while since we last saw you on TaxoBuddy! We've been working hard to make our professional tax intelligence even more powerful and easier to use.</p>
                        
                        <div class="feature-box">
                            <strong>What's waiting for you:</strong>
                            <ul style="margin: 12px 0; padding-left: 20px;">
                                <li>Updated tax case law database</li>
                                <li>Better Draft Reply generation</li>
                                <li>Improved document analysis speed</li>
                            </ul>
                        </div>
                        
                        <p>Ready to jump back in? Click the button below to access your account.</p>
                        
                        <div style="text-align: center;">
                            <a href="{login_url}" class="button">Back to TaxoBuddy</a>
                        </div>
                        
                        <p>If you have any feedback on how we can improve, just reply to this email. We'd love to hear from you!</p>
                    </div>
                    <div class="footer">
                        © {datetime.now().year} TaxoBuddy Professional Tax Intelligence.<br>
                        You received this because you are a registered user of TaxoBuddy.
                    </div>
                </div>
            </body>
        </html>
        """
        return EmailService.send_email(subject, html_content, email)
