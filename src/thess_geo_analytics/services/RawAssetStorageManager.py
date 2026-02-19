from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal, Tuple

from thess_geo_analytics.services.CdseAssetDownloader import CdseAssetDownloader
from thess_geo_analytics.utils.GcsClient import GcsClient

StorageMode = Literal[
    "url_to_local",            # CDSE → local only
    "url_to_gcs_keep_local",   # CDSE → local + GCS (keep local)
    "url_to_gcs_drop_local",   # CDSE → local + GCS (then delete local)
    "gcs_to_local",            # GCS → local (rehydrate cache)
]


@dataclass
class RawAssetStorageManager:
    """
    Handles where raw bands live (local / GCS) and how to (re)hydrate them.

    Modes:
      - url_to_local:
          Use CDSE URL, download to local if missing. No GCS involved.

      - url_to_gcs_keep_local:
          Use CDSE URL, download to local if missing, upload to GCS (if needed),
          keep local file.

      - url_to_gcs_drop_local:
          Use CDSE URL, download to local if missing, upload to GCS (if needed),
          then delete local file (GCS-only storage).

      - gcs_to_local:
          Ignore CDSE URL, use existing gs:// URL to restore local from GCS.
    """

    mode: StorageMode
    downloader: CdseAssetDownloader
    gcs_client: Optional[GcsClient] = None
    gcs_prefix: str = "raw_s2"

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
        Ensure that `local_path` exists according to the selected mode.

        Returns:
          (ok, new_gcs_url)

          - ok: True if we consider the local band "available" (exist or restored).
          - new_gcs_url: a gs:// URL if we uploaded or reused one, otherwise None.
        """

        local_path = local_path

        # 0) If local already exists and mode doesn't force otherwise
        if local_path.exists() and self.mode != "gcs_to_local":
            # Local is already there, we may still want a GCS URL in some modes.
            if self.mode.startswith("url_to_gcs") and self.gcs_client is not None:
                # If we already have a gs://, reuse it
                if gcs_url:
                    return True, gcs_url
                remote_path = self._default_remote_path(scene_id, band)
                url_out = self._upload_if_needed(local_path, remote_path)
                return True, url_out
            return True, gcs_url

        # 1) GCS → local mode
        if self.mode == "gcs_to_local":
            if not self.gcs_client:
                print("[ERROR] gcs_to_local mode but no GcsClient provided.")
                return False, gcs_url
            if not gcs_url:
                print(f"[WARN] gcs_to_local mode but no gcs_url for {scene_id} {band}.")
                return False, gcs_url

            try:
                remote_path = self._remote_path_from_gs_url(gcs_url)
            except ValueError as e:
                print(f"[WARN] gcs_url parse error for {scene_id} {band}: {e}")
                return False, gcs_url

            try:
                self.gcs_client.download(remote_path, local_path)
                print(f"[OK] Restored from GCS → {local_path}")
                return True, gcs_url
            except Exception as e:
                print(f"[WARN] Failed to download from GCS for {scene_id} {band}: {e}")
                return False, gcs_url

        # 2) URL-driven modes
        if url is None or url == "" or str(url) == "nan":
            print(f"[WARN] No URL provided for {scene_id} {band} in mode {self.mode}.")
            return False, gcs_url

        # If local missing, download via CDSE
        if not local_path.exists():
            try:
                self.downloader.download(url, local_path)
            except Exception as e:
                print(f"[WARN] Failed to download from CDSE for {scene_id} {band}: {e}")
                return False, gcs_url

        # Now local exists (assuming download succeeded)
        if self.mode == "url_to_local":
            return True, gcs_url

        if self.mode in {"url_to_gcs_keep_local", "url_to_gcs_drop_local"}:
            if not self.gcs_client:
                print("[WARN] GCS mode but no GcsClient provided — skipping upload.")
                return True, gcs_url

            # Use existing gcs_url if present, otherwise upload.
            if gcs_url:
                url_out = gcs_url
            else:
                remote_path = self._default_remote_path(scene_id, band)
                url_out = self._upload_if_needed(local_path, remote_path)

            # Delete local if requested
            if self.mode == "url_to_gcs_drop_local":
                try:
                    local_path.unlink(missing_ok=True)
                    print(f"[INFO] Deleted local {scene_id} {band} after GCS upload.")
                except Exception as e:
                    print(f"[WARN] Failed to delete local file {local_path}: {e}")

            return True, url_out

        print(f"[WARN] Unknown storage mode: {self.mode}")
        return False, gcs_url

    # -----------------
    # Internals
    # -----------------
    def _default_remote_path(self, scene_id: str, band: str) -> str:
        # e.g. raw_s2/<scene_id>/B04.tif
        return f"{self.gcs_prefix}/{scene_id}/{band}.tif"

    def _upload_if_needed(self, local_path: Path, remote_path: str) -> str:
        assert self.gcs_client is not None
        # Always upload (idempotent from caller perspective)
        url = self.gcs_client.upload(local_path, remote_path)
        return url

    def _remote_path_from_gs_url(self, gcs_url: str) -> str:
        """
        Convert gs://bucket/path → "path" (relative path inside the bucket).
        """
        assert self.gcs_client is not None
        bucket = self.gcs_client.bucket
        prefix = f"gs://{bucket}/"
        if not gcs_url.startswith(prefix):
            raise ValueError(f"gcs_url bucket mismatch: expected prefix {prefix}, got {gcs_url}")
        return gcs_url[len(prefix):]
