from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple
from urllib.parse import urlparse

import boto3


@dataclass(frozen=True)
class S3Config:
    endpoint_url: str = "https://eodata.dataspace.copernicus.eu"
    bucket: str = "eodata"
    region_name: str = "default"


class S3AssetDownloader:
    

    def __init__(self, cfg: S3Config | None = None) -> None:
        self.cfg = cfg or S3Config()

        access_key = os.environ.get("CDSE_S3_ACCESS_KEY")
        secret_key = os.environ.get("CDSE_S3_SECRET_KEY")
        if not access_key or not secret_key:
            raise EnvironmentError(
                "Missing S3 credentials. Set env vars CDSE_S3_ACCESS_KEY and CDSE_S3_SECRET_KEY."
            )

        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=self.cfg.endpoint_url,
            region_name=self.cfg.region_name,
        )

    def _parse_s3_url(self, s3_url: str) -> Tuple[str, str]:
        # s3://eodata/path/to/file.jp2
        parsed = urlparse(s3_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise ValueError(f"Invalid s3 url: {s3_url}")
        return bucket, key

    def download(self, s3_url: str, dest: Path) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dest.exists() and dest.stat().st_size > 0:
            return dest

        bucket, key = self._parse_s3_url(s3_url)
        self.client.download_file(bucket, key, str(dest))
        return dest
