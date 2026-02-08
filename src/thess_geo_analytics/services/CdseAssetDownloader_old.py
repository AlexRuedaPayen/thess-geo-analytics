from __future__ import annotations
from pathlib import Path
import requests

class CdseAssetDownloader:
    def __init__(self, token_service) -> None:
        self.token_service = token_service

    def download(self, href: str, out_path: Path) -> Path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        token = self.token_service.get_token()
        headers = {"Authorization": f"Bearer {token}"}

        with requests.get(href, headers=headers, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return out_path
