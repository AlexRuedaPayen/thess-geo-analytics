from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Literal
import json

import pandas as pd

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.core.settings import CACHE_S2_DIR, DEFAULT_COLLECTION
from thess_geo_analytics.services.CdseStacService import CdseStacService
from thess_geo_analytics.services.StacAssetResolver import StacAssetResolver


SortMode = Literal["as_is", "cloud_then_time", "time"]


@dataclass(frozen=True)
class AssetsManifestBuildParams:
    """
    Parameters controlling assets manifest creation.
    """

    max_scenes: Optional[int] = None
    date_start: Optional[str] = None
    sort_mode: SortMode = "as_is"

    collection: str = DEFAULT_COLLECTION
    cache_root: Path = CACHE_S2_DIR


class AssetsManifestBuilder:
    """
    Builds the assets manifest DataFrame.

    Output columns:

        scene_id
        datetime
        cloud_cover
        href_b04
        href_b08
        href_scl
        local_b04
        local_b08
        local_scl

    Responsibilities:
        • resolve STAC items
        • resolve asset URLs
        • define local file locations

    Non-responsibilities:
        • downloading
        • validation
        • writing files
    """

    REQUIRED_SCENES_COLS = {"id", "datetime", "cloud_cover"}

    def __init__(
        self,
        *,
        stac_service: CdseStacService,
        band_resolution: int = 10,
        resolver: StacAssetResolver | None = None,
    ) -> None:

        self.stac = stac_service
        self.band_resolution = int(band_resolution)

        self.resolver = resolver or StacAssetResolver(
            band_resolution=self.band_resolution
        )

    # ---------------------------------------------------------
    # Core builder
    # ---------------------------------------------------------
    def build_assets_manifest_df(
        self,
        scenes_df: pd.DataFrame,
        params: AssetsManifestBuildParams,
    ) -> pd.DataFrame:

        missing = self.REQUIRED_SCENES_COLS - set(scenes_df.columns)
        if missing:
            raise ValueError(f"Scenes catalog missing columns: {sorted(missing)}")

        df = scenes_df.copy()

        # Normalize datetime
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
        df = df.dropna(subset=["datetime"]).reset_index(drop=True)

        # Date filtering
        if params.date_start:
            start = pd.to_datetime(params.date_start, utc=True)
            df = df[df["datetime"] >= start]

        if df.empty:
            raise ValueError("No scenes left after datetime filtering.")

        # Sorting
        if params.sort_mode == "cloud_then_time":
            df = df.sort_values(["cloud_cover", "datetime"], ascending=[True, True])

        elif params.sort_mode == "time":
            df = df.sort_values(["datetime"], ascending=[True])

        # Scene cap
        if params.max_scenes is not None:
            df = df.head(int(params.max_scenes))

        # -------------------------
        # STAC cache directory
        # -------------------------

        stac_cache_dir = params.cache_root / "stac_items"
        stac_cache_dir.mkdir(parents=True, exist_ok=True)

        rows: List[dict] = []

        for _, r in df.iterrows():

            item_id = str(r["id"])

            cache_path = stac_cache_dir / f"{item_id}.json"

            item_json = None

            # Try cache
            if cache_path.exists():
                try:
                    with cache_path.open("r", encoding="utf-8") as f:
                        item_json = json.load(f)
                except Exception:
                    item_json = None

            # Fetch from STAC
            if item_json is None:

                item_json = self.stac.fetch_item(params.collection, item_id)

                try:
                    with cache_path.open("w", encoding="utf-8") as f:
                        json.dump(item_json, f)
                except Exception:
                    pass

            hrefs = self.resolver.resolve_b04_b08_scl(item_json)

            scene_dir = RepoPaths.run_root() / "raw" / "s2" / item_id
            scene_dir.mkdir(parents=True, exist_ok=True)

            rows.append(
                {
                    "scene_id": item_id,
                    "datetime": r["datetime"].isoformat(),
                    "cloud_cover": (
                        float(r["cloud_cover"])
                        if pd.notna(r["cloud_cover"])
                        else None
                    ),
                    "href_b04": hrefs.get("href_b04"),
                    "href_b08": hrefs.get("href_b08"),
                    "href_scl": hrefs.get("href_scl"),
                    "local_b04": str(scene_dir / "B04.tif"),
                    "local_b08": str(scene_dir / "B08.tif"),
                    "local_scl": str(scene_dir / "SCL.tif"),
                }
            )

        return pd.DataFrame(rows)

    # ---------------------------------------------------------
    # Smoke test
    # ---------------------------------------------------------
    @staticmethod
    def smoke_test() -> None:

        print("=== AssetsManifestBuilder Smoke Test ===")

        from thess_geo_analytics.utils.RepoPaths import RepoPaths

        scenes_csv = RepoPaths.table("scenes_selected.csv")

        if not scenes_csv.exists():
            raise FileNotFoundError(f"Missing scenes catalog: {scenes_csv}")

        scenes_df = pd.read_csv(scenes_csv)

        # real STAC service
        stac = CdseStacService()

        builder = AssetsManifestBuilder(
            stac_service=stac,
            band_resolution=10,
        )

        params = AssetsManifestBuildParams(
            max_scenes=5,
            sort_mode="as_is",
        )

        out_df = builder.build_assets_manifest_df(scenes_df, params)

        print("[OK] rows:", len(out_df))
        print(out_df.head(3)[["scene_id", "datetime", "href_b04", "local_b04"]])

        missing_hrefs = (
            out_df[["href_b04", "href_b08", "href_scl"]]
            .isna()
            .any(axis=1)
            .sum()
        )

        if missing_hrefs:
            print(f"[WARN] {missing_hrefs} rows have missing hrefs")

        print("✓ Smoke test OK")


if __name__ == "__main__":
    AssetsManifestBuilder.smoke_test()