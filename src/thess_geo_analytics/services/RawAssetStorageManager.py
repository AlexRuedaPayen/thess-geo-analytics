from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal, Tuple

from thess_geo_analytics.services.CdseAssetDownloader import CdseAssetDownloader
from thess_geo_analytics.utils.GcsClient import GcsClient

StorageMode = Literal[
    "url_to_local",
    "url_to_gcs_keep_local",
    "url_to_gcs_drop_local",
    "gcs_to_local",
]


@dataclass
class RawAssetStorageManager:
    """
    Orchestrates where raw bands (B04/B08/SCL) live and how they are fetched.

    Modes:
      - "url_to_local":
            Use href_* URLs, download to local if missing, never touch GCS.

      - "url_to_gcs_keep_local":
            Use href_* URLs to ensure local exists, then upload to GCS if not
            already there. Local copy kept.

      - "url_to_gcs_drop_local":
            Same as above, but delete local file after successful upload.

      - "gcs_to_local":
            Ignore href_* and assume gcs_* URL is present; download from GCS
            to local if missing.
    """

    mode: StorageMode
    downloader: Optional[CdseAssetDownloader] = None
    gcs_client: Optional[GcsClient] = None
    gcs_prefix: str = "raw_s2"  # base folder in the bucket

    _disable_gcs_upload: bool = False

    def ensure_local(
        self,
        *,
        url: Optional[str],
        local_path: Path,
        scene_id: str,
        band: str,
        gcs_url: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Ensure a local file exists for this (scene_id, band).

        Returns:
            (ok, new_gcs_url):
                ok          -> True if we have a local file that is usable
                new_gcs_url -> updated GCS URL (if this call caused an upload),
                               or existing one if nothing changed.
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # ----- Mode: local only (no GCS at all) -----
        if self.mode == "url_to_local":
            if not local_path.exists():
                if not url:
                    return False, None
                self._download_from_url(url, local_path, scene_id, band)
            # never upload
            return local_path.exists(), None

        # ----- Modes that USE GCS -----
        if self.mode in {"url_to_gcs_keep_local", "url_to_gcs_drop_local"}:
            # Step 1: ensure local exists (from URL)
            if not local_path.exists():
                if not url:
                    # No way to get the file
                    return False, gcs_url
                self._download_from_url(url, local_path, scene_id, band)

            # Step 2: upload to GCS if needed
            new_gcs_url = gcs_url
            if self.gcs_client is not None:
                new_gcs_url = self._upload_if_needed(local_path, scene_id, band, gcs_url)

            # Step 3: maybe drop local
            if self.mode == "url_to_gcs_drop_local" and local_path.exists():
                local_path.unlink()

            return True, new_gcs_url

        # ----- Mode: restore from GCS to local -----
        if self.mode == "gcs_to_local":
            if local_path.exists():
                # local already fine, no need to re-download
                return True, gcs_url

            if not gcs_url:
                # cannot get from GCS
                return False, gcs_url

            if self.gcs_client is None:
                return False, gcs_url

            # gcs_url is like gs://bucket/prefix/... -> we only keep the object name part
            # but we know this.gcs_client already pinned to the right bucket,
            # so we just take the path after "gs://<bucket>/".
            remote_name = self._extract_object_name_from_gs_url(gcs_url)
            try:
                self.gcs_client.download(remote_name, local_path)
            except Exception as e:
                print(f"[WARN] Failed to download from GCS ({scene_id} {band}): {e}")
                return False, gcs_url

            return local_path.exists(), gcs_url

        # Should never happen
        raise ValueError(f"Unsupported StorageMode: {self.mode}")

    # -----------------------
    # internals
    # -----------------------
    def _download_from_url(self, url: str, local_path: Path, scene_id: str, band: str) -> None:
        if self.downloader is None:
            raise RuntimeError("RawAssetStorageManager.downloader is not set")

        try:
            # print(f"[DL] {scene_id} {band} → {local_path.name}")
            self.downloader.download(url, local_path)
        except Exception as e:
            raise RuntimeError(f"Failed to download {scene_id} {band} from {url}: {e}") from e

    def _upload_if_needed(self, local_path: Path, scene_id: str, band: str, gcs_url: Optional[str]) -> Optional[str]:
        """
        Upload local file to GCS if not already present or if we had no GCS URL.
        Never re-uploads blindly; checks existence first.
        """
        if self.gcs_client is None or self._disable_gcs_upload:
            return gcs_url

        # If we already had a real GCS URL, assume it's valid and skip re-upload.
        if isinstance(gcs_url, str) and gcs_url.strip():
            return gcs_url

        object_name = f"{self.gcs_prefix}/{scene_id}/{band}.tif"

        # If it already exists, just return its URL
        if self.gcs_client.exists(object_name):
            return f"gs://{self.gcs_client.bucket}/{object_name}"

        try:
            url_out = self.gcs_client.upload(local_path, object_name)
            # print(f"[OK] GCS upload {scene_id} {band} → {url_out}")
            return url_out
        except Exception as e:
            print(f"[WARN] Failed to upload {scene_id} {band} to GCS: {e}")
            # After the first network-ish failure, stop trying GCS this run
            self._disable_gcs_upload = True
            return gcs_url

    @staticmethod
    def _extract_object_name_from_gs_url(gs_url: str) -> str:
        # "gs://bucket/path/to/object.tif" -> "path/to/object.tif"
        if not gs_url.startswith("gs://"):
            return gs_url
        without_scheme = gs_url[5:]
        parts = without_scheme.split("/", 1)
        if len(parts) == 1:
            return ""
        return parts[1]