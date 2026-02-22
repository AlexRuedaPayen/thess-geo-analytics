from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Tuple

from tqdm import tqdm

from thess_geo_analytics.utils.GcsClient import GcsClient


def find_ndvi_composites(cogs_dir: Path, aoi_id: str) -> List[Path]:
    """
    Find NDVI monthly/quarterly composite COGs produced by the NDVI Monthly Composite pipeline.

    Pattern (case-insensitive):
        ndvi_YYYY-MM_<aoi_id>.tif
        ndvi_YYYY-Qn_<aoi_id>.tif

    Excludes:
        - ndvi_anomaly_*
        - ndvi_climatology_*
        - anything not matching the YYYY-MM / YYYY-Qn pattern
    """
    cogs_dir = cogs_dir.resolve()
    if not cogs_dir.exists():
        raise SystemExit(f"COGs directory does not exist: {cogs_dir}")

    pattern = re.compile(
        rf"^ndvi_(\d{{4}}-(\d{{2}}|Q[1-4]))_{re.escape(aoi_id)}\.tif$",
        re.IGNORECASE,
    )

    composites: List[Path] = []

    for path in cogs_dir.iterdir():
        if not path.is_file():
            continue

        name = path.name
        low = name.lower()

        # Hard exclude anomaly / climatology products
        if "anomaly" in low or "climatology" in low:
            continue

        if pattern.match(name):
            composites.append(path)

    composites.sort()
    return composites


def build_upload_list(
    composites: List[Path],
    png_dir: Path | None,
    remote_prefix: str,
) -> List[Tuple[Path, str]]:
    """
    For each composite COG, upload:
      - the .tif file
      - the .png with the same stem if it exists in png_dir

    remote path = <remote_prefix>/<filename>
    """
    uploads: List[Tuple[Path, str]] = []

    for tif_path in composites:
        # COG
        tif_remote = f"{remote_prefix}/{tif_path.name}"
        uploads.append((tif_path, tif_remote))

        # Optional PNG preview
        if png_dir is not None:
            png_candidate = png_dir / (tif_path.stem + ".png")
            if png_candidate.exists():
                png_remote = f"{remote_prefix}/{png_candidate.name}"
                uploads.append((png_candidate, png_remote))

    return uploads


def main() -> None:
    p = argparse.ArgumentParser(
        description="Upload NDVI Monthly/Quarterly composite outputs (COGs + PNG previews) to GCS."
    )

    # Where to upload
    p.add_argument("--bucket", required=True, help="Target GCS bucket.")
    p.add_argument("--credentials", default=None, help="Path to service account JSON.")
    p.add_argument(
        "--remote-prefix",
        default="ndvi/composites",
        help="Remote prefix in GCS (default: ndvi/composites).",
    )

    # Local data
    p.add_argument(
        "--cogs-dir",
        default="outputs/cogs",
        help="Directory containing NDVI composite COGs (default: outputs/cogs).",
    )
    p.add_argument(
        "--png-dir",
        default="outputs/png",
        help="Directory containing NDVI composite previews (default: outputs/png). "
             "If it does not exist, only COGs will be uploaded.",
    )
    p.add_argument(
        "--aoi-id",
        required=True,
        help="AOI id used in filenames (e.g. el522).",
    )

    args = p.parse_args()

    cogs_dir = Path(args.cogs_dir)
    png_dir = Path(args.png_dir)
    if not png_dir.exists():
        png_dir = None  # silently skip PNGs if folder is missing

    # 1) Discover composites from filenames only (no pipeline, no manifest)
    composites = find_ndvi_composites(cogs_dir, aoi_id=args.aoi_id)
    if not composites:
        raise SystemExit(f"No NDVI composites found in {cogs_dir} for aoi_id={args.aoi_id}")

    print(f"[INFO] Found {len(composites)} NDVI composite COGs in {cogs_dir}")

    # 2) Build upload list (tifs + pngs if present)
    uploads = build_upload_list(composites, png_dir, remote_prefix=args.remote_prefix)
    print(f"[INFO] Total files to upload (COGs + PNGs): {len(uploads)}")

    # 3) Upload with GcsClient + tqdm
    gcs = GcsClient(bucket=args.bucket, credentials=args.credentials)

    with tqdm(total=len(uploads), desc="NDVI composites", unit="file") as bar:
        for local_path, remote_path in uploads:
            gcs.upload(local_path, remote_path)
            bar.update(1)

    print("\n✓ DONE — NDVI Monthly/Quarterly composites uploaded to:")
    print(f"  gs://{args.bucket}/{args.remote_prefix}/")


if __name__ == "__main__":
    main()