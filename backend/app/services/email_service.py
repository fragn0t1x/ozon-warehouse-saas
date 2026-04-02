from __future__ import annotations

import asyncio
import logging
import smtplib
from email.message import EmailMessage

from app.config import settings


logger = logging.getLogger(__name__)


class EmailService:
    def __init__(self) -> None:
        self.host = settings.SMTP_HOST
        self.port = settings.SMTP_PORT
        self.user = settings.SMTP_USER
        self.password = settings.SMTP_PASSWORD.get_secret_value() if settings.SMTP_PASSWORD else None
        self.sender = settings.SMTP_FROM or settings.SMTP_USER

    @property
    def configured(self) -> bool:
        return bool(self.host and self.port and self.sender)

    async def send_message(
        self,
        *,
        to_email: str,
        subject: str,
        text: str,
        html: str | None = None,
    ) -> bool:
        if not self.configured:
            logger.warning("SMTP is not configured; email delivery skipped")
            return False

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.sender
        message["To"] = to_email
        message.set_content(text)
        if html:
            message.add_alternative(html, subtype="html")

        return await asyncio.to_thread(self._send_sync, message)

    def _send_sync(self, message: EmailMessage) -> bool:
        try:
            with smtplib.SMTP(self.host, self.port, timeout=20) as server:
                server.ehlo()
                try:
                    server.starttls()
                    server.ehlo()
                except Exception:
                    logger.info("SMTP server does not support STARTTLS or it is not required")

                if self.user and self.password:
                    server.login(self.user, self.password)

                server.send_message(message)
            return True
        except Exception as exc:
            logger.error(f"Email send failed: {exc}")
            return False
