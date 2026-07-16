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


def _load_token_from_file(token_file, email, password):
    """Load and decrypt a portal token from disk (blocking I/O + crypto)."""
    if not os.path.exists(token_file):
        logger.info("No token file found. %s", os.path.abspath(token_file))
        return None

    with open(token_file, "r", encoding="utf-8") as token_file_handle:
        token_data = json.load(token_file_handle)

    exp = token_data.get("expires")
    if not exp:
        return None

    token_exp = datetime.datetime.fromisoformat(exp.replace("Z", "+00:00"))
    logger.info("Token expires in %s", token_exp)
    if not datetime.datetime.now(datetime.timezone.utc) < token_exp:
        return None

    _created_time, session_token = decrypt_wrapped_token(
        token_data.get("token"), email, password
    )

    return {
        "login_time": token_data.get("token_timestamp"),
        "token_exp": token_exp,
        "session_token": session_token,
    }


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

    def _apply_session_token(self, token_payload):
        self.login_time = token_payload["login_time"]
        self.token_exp = token_payload["token_exp"]
        self.session_token = token_payload["session_token"]

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

    async def async_get_session_token(self, email=None, password=None):
        if email is None:
            email = self.email
        if password is None:
            password = self.password

        token_payload = await asyncio.to_thread(
            _load_token_from_file, TOKEN_FILE, email, password
        )
        if token_payload is None:
            return False

        self._apply_session_token(token_payload)
        return True

    async def async_check_session(self):
        if self.session is None:
            return await self.async_get_session_token()
        if (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        ) > self.token_exp:
            return await self.async_get_session_token()

        logger.info("Session is still valid, so we don't need to get a new one")
        return True
