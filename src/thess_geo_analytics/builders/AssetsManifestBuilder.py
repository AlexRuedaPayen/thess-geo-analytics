from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Literal
import json  # <-- NEW

import pandas as pd

from thess_geo_analytics.core.settings import CACHE_S2_DIR, DEFAULT_COLLECTION
from thess_geo_analytics.services.CdseStacService import CdseStacService
from thess_geo_analytics.services.StacAssetResolver import StacAssetResolver


SortMode = Literal["as_is", "cloud_then_time", "time"]


@dataclass(frozen=True)
class AssetsManifestBuildParams:
    """
    Build an assets manifest for the (already curated) scenes in scenes_selected.csv.

    - If you want "all", keep max_scenes=None.
    - If you want to cap for dev, set max_scenes=10 (etc).
    - If you want a time slice, set date_start (YYYY-MM-DD).
    """
    max_scenes: Optional[int] = None
    collection: str = DEFAULT_COLLECTION
    cache_root: Path = CACHE_S2_DIR

    # optional filtering
    date_start: Optional[str] = None  # "YYYY-MM-DD"

    # optional ordering
    sort_mode: SortMode = "as_is"


class AssetsManifestBuilder:
    """
    Builds the assets manifest DataFrame:
      scene_id, datetime, cloud_cover,
      href_b04, href_b08, href_scl,
      local_b04, local_b08, local_scl

    No file I/O and no downloads here, except for the STAC item cache
    (JSON per scene) to avoid repeated network calls across runs.
    """

    REQUIRED_SCENES_COLS = {"id", "datetime", "cloud_cover"}

    def __init__(
        self,
        *,
        band_resolution: int = 10,
        stac: CdseStacService | None = None,
        resolver: StacAssetResolver | None = None,
    ) -> None:
        """
        band_resolution:
          - 10 => prefer B04_10m / B08_10m (deep mode)
          - 20 => prefer B04_20m / B08_20m (dev mode)
        """
        self.band_resolution = int(band_resolution)
        self.stac = stac or CdseStacService()
        self.resolver = resolver or StacAssetResolver(
            band_resolution=self.band_resolution
        )

    def build_assets_manifest_df(
        self, scenes_df: pd.DataFrame, params: AssetsManifestBuildParams
    ) -> pd.DataFrame:
        missing = self.REQUIRED_SCENES_COLS - set(scenes_df.columns)
        if missing:
            raise ValueError(f"Scenes catalog missing columns: {sorted(missing)}")

        df = scenes_df.copy()

        # normalize datetime
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
        df = df.dropna(subset=["datetime"]).reset_index(drop=True)

        # optional date filtering: only date_start now
        if params.date_start:
            start = pd.to_datetime(params.date_start, utc=True)
            df = df[df["datetime"] >= start]

        if df.empty:
            raise ValueError("No scenes left after datetime parsing / date filtering.")

        # optional sorting
        if params.sort_mode == "cloud_then_time":
            df = df.sort_values(["cloud_cover", "datetime"], ascending=[True, True])
        elif params.sort_mode == "time":
            df = df.sort_values(["datetime"], ascending=[True])

        # optional cap
        if params.max_scenes is not None:
            df = df.head(int(params.max_scenes))

        # -------------------------------
        # STAC item cache initialisation
        # -------------------------------
        stac_cache_dir = params.cache_root / "stac_items"
        stac_cache_dir.mkdir(parents=True, exist_ok=True)

        rows: List[dict] = []

        for _, r in df.iterrows():
            item_id = str(r["id"])

            # Cache path for this scene's STAC item JSON
            cache_path = stac_cache_dir / f"{item_id}.json"

            # Try cache first
            item_json = None
            if cache_path.exists():
                try:
                    with cache_path.open("r", encoding="utf-8") as f:
                        item_json = json.load(f)
                    # Optional: you could add a debug print on first hit
                    # print(f"[STAC CACHE] Hit for {item_id}")
                except Exception:
                    # Corrupted cache → ignore and refetch
                    item_json = None

            # If not in cache or failed to read → fetch from STAC and store
            if item_json is None:
                # First run or cache miss: actual network call
                item_json = self.stac.fetch_item(params.collection, item_id)

                try:
                    with cache_path.open("w", encoding="utf-8") as f:
                        json.dump(item_json, f)
                except Exception:
                    # Cache write failure should not break the pipeline
                    pass

            hrefs = self.resolver.resolve_b04_b08_scl(item_json)

            scene_dir = params.cache_root / item_id

            rows.append(
                {
                    "scene_id": item_id,
                    "datetime": r["datetime"].isoformat(),
                    "cloud_cover": float(r["cloud_cover"]) if pd.notna(r["cloud_cover"]) else None,
                    "href_b04": hrefs.get("href_b04"),
                    "href_b08": hrefs.get("href_b08"),
                    "href_scl": hrefs.get("href_scl"),
                    "local_b04": str(scene_dir / "B04.tif"),
                    "local_b08": str(scene_dir / "B08.tif"),
                    "local_scl": str(scene_dir / "SCL.tif"),
                }
            )

        out = pd.DataFrame(rows)

        return out

    @staticmethod
    def smoke_test() -> None:
        print("=== AssetsManifestBuilder Smoke Test (scenes_selected.csv) ===")
        from thess_geo_analytics.utils.RepoPaths import RepoPaths

        scenes_csv = RepoPaths.table("scenes_selected.csv")
        if not scenes_csv.exists():
            raise FileNotFoundError(f"Missing scenes catalog: {scenes_csv}")

        scenes_df = pd.read_csv(scenes_csv)
        builder = AssetsManifestBuilder(band_resolution=10)

        params = AssetsManifestBuildParams(
            max_scenes=5,
            sort_mode="as_is",
        )

        out_df = builder.build_assets_manifest_df(scenes_df, params)
        print("[OK] rows:", len(out_df))
        print(out_df.head(3)[["scene_id", "datetime", "href_b04", "local_b04"]])

        missing_hrefs = out_df[["href_b04", "href_b08", "href_scl"]].isna().any(axis=1).sum()
        if missing_hrefs:
            print(f"[WARN] {missing_hrefs} rows have missing hrefs (resolver mismatch)")

        print("✓ Smoke test OK")


if __name__ == "__main__":
    AssetsManifestBuilder.smoke_test()