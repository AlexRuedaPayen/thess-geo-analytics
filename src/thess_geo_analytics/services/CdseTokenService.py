from __future__ import annotations

from typing import Optional

from thess_geo_analytics.core.constants import CDSE_TOKEN_URL
from thess_geo_analytics.core.HttpClient import HttpClient, HttpConfig
from thess_geo_analytics.core.settings import CDSE_USERNAME, CDSE_PASSWORD, CDSE_TOTP


class CdseTokenService:
    def __init__(self, http: HttpClient | None = None) -> None:
        self._token: Optional[str] = None
        self.http = http or HttpClient(HttpConfig())

    def get_token(self) -> str:
        if self._token:
            return self._token

        if not CDSE_USERNAME or not CDSE_PASSWORD:
            raise EnvironmentError("Missing CDSE_USERNAME / CDSE_PASSWORD in environment (.env).")

        data = {
            "client_id": "cdse-public",
            "grant_type": "password",
            "username": CDSE_USERNAME,
            "password": CDSE_PASSWORD,
        }
        if CDSE_TOTP:
            data["totp"] = CDSE_TOTP

        r = self.http.post(CDSE_TOKEN_URL, data=data)
        token = r.json().get("access_token")
        if not token:
            raise RuntimeError("Token response missing access_token")
        self._token = token
        return self._token

    @staticmethod
    def smoke_test() -> None:
        print("=== CdseTokenService Smoke Test ===")
        svc = CdseTokenService()
        tok = svc.get_token()
        print("[OK] Token retrieved.")
        print("Token prefix:", tok[:20], "...")
        print("✓ Smoke test OK")


if __name__ == "__main__":
    CdseTokenService.smoke_test()
