import smtplib
from .config import Config
import logging
from typing import Optional

def send_email(config: Config, recipient: str, subject: str, body: str) -> None:
    """
    Send an email using the credentials in the config object.
    """
    if not (config.gmail_user and config.gmail_password and recipient):
        return

    FROM = config.gmail_user
    TO = recipient if isinstance(recipient, list) else [recipient]
    SUBJECT = subject
    TEXT = body

    message = f"From: {FROM}\nTo: {', '.join(TO)}\nSubject: {SUBJECT}\n\n{TEXT}"
    server_ssl = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server_ssl.ehlo()
    server_ssl.login(config.gmail_user, config.gmail_password)
    server_ssl.sendmail(FROM, TO, message)
    server_ssl.close()

    logging.info(f'Sent notification email to {recipient}')

def send_admin_email(config: Config, body: str) -> None:
    """
    Send an admin notification email, using the config's notification target.
    """
    if 'ERROR' in body:
        subject = f'{config.prog_name} encountered errors'
    else:
        subject = f'{config.prog_name} completed without errors'
    send_email(config, config.notification_target_email, subject, body) 