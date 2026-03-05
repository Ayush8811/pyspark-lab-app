import os
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

# We look for these in environment variables (e.g. Hugging Face Secrets or .env)
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

def send_recovery_email(to_email: str, recovery_code: str):
    """
    Sends a 6-digit recovery code to the user's email.
    If no SMTP server is configured, it falls back to printing the code
    in the terminal so developers/users aren't locked out during testing.
    """
    subject = "PySpark Platform - Password Reset Code"
    body = f"Hello,\n\nYour password reset code is: {recovery_code}\n\nThis code will expire in 15 minutes.\n\nIf you did not request this, please ignore this email.\n\nThanks,\nThe PySpark Platform Team"
    
    if not SMTP_SERVER or not SMTP_USER or not SMTP_PASSWORD:
        # Development fallback
        print("="*60)
        print("EMAIL SIMULATION - NO SMTP CONFIGURED")
        print(f"To: {to_email}")
        print(f"Subject: {subject}")
        print("-" * 60)
        print(body)
        print("="*60)
        return True
        
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From'] = SMTP_USER
        msg['To'] = to_email
        
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False
