from __future__ import annotations

from src.thess_geo_analytics.pipelines.AssetsManifestBuilder import AssetsManifestBuilder


class BuildAssetsManifest:
    def __init__(self) -> None:
        pass

    def run(self) -> None:
        AssetsManifestBuilder(month="2026-01", max_scenes=10).run()


if __name__ == "__main__":
    BuildAssetsManifest().run()
