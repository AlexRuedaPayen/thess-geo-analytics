from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / ".env")
import os
import requests


TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

class CdseTokenService:
    def __init__(self) -> None:
        self._token: str | None = None

    def get_token(self) -> str:
        if self._token:
            return self._token

        username = os.getenv("CDSE_USERNAME")
        password = os.getenv("CDSE_PASSWORD")
        totp = os.getenv("CDSE_TOTP")

        if not username or not password:
            raise EnvironmentError("Missing CDSE_USERNAME / CDSE_PASSWORD in environment (.env).")

        data = {
            "client_id": "cdse-public",
            "grant_type": "password",
            "username": username,
            "password": password,
        }
        if totp:
            data["totp"] = totp

        r = requests.post(TOKEN_URL, data=data, timeout=60)
        r.raise_for_status()
        self._token = r.json()["access_token"]
        return self._token
