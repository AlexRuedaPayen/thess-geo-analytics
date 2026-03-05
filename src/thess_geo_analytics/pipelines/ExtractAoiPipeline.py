from __future__ import annotations

import re
from pathlib import Path

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.services.NutsService import NutsService
from thess_geo_analytics.builders.AoiBuilder import AoiBuilder


def slugify(text: str) -> str:
    text = text.strip()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^A-Za-z0-9_\-]+", "", text)
    return text


class ExtractAoiPipeline:
    """
    Orchestrates:
      NutsService -> select region -> AoiBuilder -> export aoi/*.geojson
    """

    def __init__(
        self,
        nuts_service: NutsService | None = None,
        builder: AoiBuilder | None = None,
    ) -> None:

        # default dependencies
        self.nuts = nuts_service or NutsService()
        self.builder = builder or AoiBuilder()

    def run(self, region_name: str) -> Path:

        nuts_code = self.nuts.find_code_by_name_exact(region_name)

        gdf = self.nuts.get_by_code(nuts_code)

        aoi_gdf = self.builder.build_aoi(gdf)

        out_name = f"{nuts_code}_{slugify(region_name)}.geojson"
        out_path = RepoPaths.aoi(out_name)

        self.builder.export_geojson(aoi_gdf, out_path)

        print(f"[OUTPUT] AOI extracted for {nuts_code} ({region_name})")
        print(f"[OUTPUT] Saved to: {out_path}")

        return out_path