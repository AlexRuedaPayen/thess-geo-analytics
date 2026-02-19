from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import tempfile
import uuid

from google.cloud import storage


@dataclass
class GcsClient:
    """
    Simple GCS wrapper for uploading/downloading rasters, tables, and figures.

    Example:
        gcs = GcsClient(bucket="ndvi-thess-bucket",
                        credentials="~/.gcp/thess-geo-analytics-nvdi.json")
        gcs.upload("outputs/cogs/ndvi_2023-08_el522.tif",
                   "ndvi/composites/monthly/ndvi_2023-08_el522.tif")
    """

    bucket: str
    credentials: Optional[str] = None

    def __post_init__(self):
        if self.credentials:
            self._client = storage.Client.from_service_account_json(self.credentials)
        else:
            # Use default credentials (e.g. on GCP VM)
            self._client = storage.Client()

        self._bucket = self._client.bucket(self.bucket)

    # ------------------------
    # Uploads
    # ------------------------
    def upload(self, local_path: str | Path, remote_path: str) -> str:
        local_path = Path(local_path)
        blob = self._bucket.blob(remote_path)
        blob.upload_from_filename(str(local_path))
        return f"gs://{self.bucket}/{remote_path}"

    def upload_bytes(self, data: bytes, remote_path: str, content_type: str | None = None) -> str:
        blob = self._bucket.blob(remote_path)
        blob.upload_from_string(data, content_type=content_type)
        return f"gs://{self.bucket}/{remote_path}"

    # ------------------------
    # Downloads
    # ------------------------
    def download(self, remote_path: str, local_path: str | Path) -> Path:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        blob = self._bucket.blob(remote_path)
        if not blob.exists():
            raise FileNotFoundError(f"No such GCS object: gs://{self.bucket}/{remote_path}")

        blob.download_to_filename(str(local_path))
        return local_path

    def download_as_bytes(self, remote_path: str) -> bytes:
        blob = self._bucket.blob(remote_path)
        if not blob.exists():
            raise FileNotFoundError(f"No such GCS object: gs://{self.bucket}/{remote_path}")
        return blob.download_as_bytes()

    # ------------------------
    # Exists / list
    # ------------------------
    def exists(self, remote_path: str) -> bool:
        return self._bucket.blob(remote_path).exists()

    def list(self, prefix: str = "") -> List[str]:
        blobs = self._client.list_blobs(self.bucket, prefix=prefix)
        return [b.name for b in blobs]

    # ------------------------
    # Delete
    # ------------------------
    def delete(self, remote_path: str) -> None:
        blob = self._bucket.blob(remote_path)
        if blob.exists():
            blob.delete()

    # ------------------------
    # Smoke Test
    # ------------------------
    @staticmethod
    def smoke_test(bucket: str, credentials: Optional[str]) -> None:
        """
        Minimal integration smoke test.

        - Uploads a small temporary file
        - Verifies existence
        - Downloads and checks integrity
        - Deletes test object

        Usage:
            GcsClient.smoke_test(
                bucket="ndvi-thess-bucket",
                credentials="~/.gcp/ndvi-service-key.json"
            )
        """
        print("=== GcsClient Smoke Test ===")

        client = GcsClient(bucket=bucket, credentials=credentials)

        # Create unique test object path
        unique_id = uuid.uuid4().hex[:8]
        remote_path = f"tests/gcs_smoke_{unique_id}.txt"
        test_content = b"hello-gcs-smoke-test"

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpfile = Path(tmpdir) / "upload.txt"
            tmpfile.write_bytes(test_content)

            # Upload
            url = client.upload(tmpfile, remote_path)
            print(f"[OK] Uploaded → {url}")

            # Exists check
            if not client.exists(remote_path):
                raise RuntimeError("Uploaded object does not exist in bucket.")
            print("[OK] Exists check passed")

            # Download
            download_path = Path(tmpdir) / "download.txt"
            client.download(remote_path, download_path)

            if download_path.read_bytes() != test_content:
                raise RuntimeError("Downloaded content mismatch.")
            print("[OK] Download round-trip verified")

            # Cleanup

            client.delete(remote_path)
            print("[OK] Cleanup successful")

        print("GcsClient smoke test OK")

if __name__ == "__main__":

    credentials_=r"C:\Users\alexr\.gcp\thess-geo-analytics-nvdi.json"
    GcsClient.smoke_test(
        bucket="thess-geo-analytics",
        credentials=credentials_ ##put your credential's location
    )
