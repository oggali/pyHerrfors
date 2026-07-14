"""Herrfors portal session and token management."""
import asyncio
import datetime
import json
import logging
import os

import aiohttp

from .const import (
    PORTAL_CHARTS_REFERER,
    PORTAL_CLIENT_ID,
    DEFAULT_TOKEN_FILE,
)
from .decode_token import decrypt_wrapped_token

logger = logging.getLogger(__name__)

TOKEN_FILE = os.getenv("TOKEN_FILE", DEFAULT_TOKEN_FILE)


class HerrforsSession:
    """Manages aiohttp session lifecycle for the Herrfors portal API."""

    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.session = None
        self.session_token = None
        self.login_time = None
        self.token_exp = None
        self.headers = None

    async def close(self):
        if self.session is not None:
            await self.session.close()
            self.session = None

    def _schedule_session_close(self, session):
        try:
            asyncio.get_running_loop().create_task(session.close())
        except RuntimeError:
            logger.warning(
                "Could not schedule aiohttp session close; no running event loop"
            )

    def get_session_token(self, email=None, password=None):
        if not os.path.exists(TOKEN_FILE):
            logger.info("No token file found. %s", os.path.abspath(TOKEN_FILE))
            return False

        with open(TOKEN_FILE, "r", encoding="utf-8") as token_file:
            token_data = json.load(token_file)

        exp = token_data.get("expires")
        if not exp:
            return False

        logger.info(
            "Token expires in %s",
            datetime.datetime.fromisoformat(exp.replace("Z", "+00:00")),
        )
        dt = datetime.datetime.fromisoformat(exp.replace("Z", "+00:00"))
        if not datetime.datetime.now(datetime.timezone.utc) < dt:
            return False

        self.login_time = token_data.get("token_timestamp")
        self.token_exp = dt

        if email is None:
            email = self.email
        if password is None:
            password = self.password

        _created_time, self.session_token = decrypt_wrapped_token(
            token_data.get("token"), email, password
        )

        if self.session is not None:
            old_session = self.session
            self.session = None
            self._schedule_session_close(old_session)

        session = aiohttp.ClientSession()
        session.cookie_jar.update_cookies(
            {"__Secure-next-auth.session-token": self.session_token}
        )
        self.session = session
        self.headers = {
            "x-client-id": PORTAL_CLIENT_ID,
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "accept": "application/json, text/plain, */*",
            "referer": PORTAL_CHARTS_REFERER,
        }
        return True

    def check_session(self):
        if self.session is None:
            return self.get_session_token()
        if (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        ) > self.token_exp:
            return self.get_session_token()

        logger.info("Session is still valid, so we don't need to get a new one")
        return True
