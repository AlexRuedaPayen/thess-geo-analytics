from __future__ import annotations

import argparse
from pathlib import Path

from tqdm import tqdm

from thess_geo_analytics.utils.GcsClient import GcsClient


def main() -> None:
    p = argparse.ArgumentParser(
        description="Upload ONLY pixel_features_7d.tif to GCS."
    )

    # Where to upload
    p.add_argument("--bucket", required=True, help="Target GCS bucket.")
    p.add_argument("--credentials", default=None, help="Path to service account JSON.")
    p.add_argument(
        "--remote-prefix",
        default="ndvi/pixel_features",
        help="Remote prefix in GCS (default: ndvi/pixel_features).",
    )

    # Local file path
    p.add_argument(
        "--pixel-features-path",
        default="outputs/cogs/pixel_features_7d.tif",
        help="Path to pixel_features_7d.tif",
    )

    args = p.parse_args()

    pixel_path = Path(args.pixel_features_path).resolve()

    # Validate file existence
    if not pixel_path.exists():
        raise SystemExit(f"ERROR: pixel_features_7d not found at: {pixel_path}")

    print(f"[INFO] Uploading pixel_features_7d raster: {pixel_path}")

    # 1-file upload list for tqdm
    files_to_upload = [
        (pixel_path, f"{args.remote_prefix}/{pixel_path.name}")
    ]

    # GCS client
    gcs = GcsClient(bucket=args.bucket, credentials=args.credentials)

    with tqdm(total=len(files_to_upload), desc="Uploading 7d Features", unit="file") as bar:
        for local_path, remote_path in files_to_upload:
            gcs.upload(local_path, remote_path)
            bar.update(1)

    print("\n✓ DONE — pixel_features_7d uploaded to:")
    print(f"  gs://{args.bucket}/{args.remote_prefix}/{pixel_path.name}")


if __name__ == "__main__":
    main()