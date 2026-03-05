from __future__ import annotations

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.pipelines.ExtractAoiPipeline import ExtractAoiPipeline
from thess_geo_analytics.utils.log_parameters import log_parameters

# Optional: types only
from thess_geo_analytics.services.NutsService import NutsService
from thess_geo_analytics.builders.AoiBuilder import AoiBuilder


PARAMETER_DOCS = {
    "region_name": "NUTS region name used to derive the AOI polygon.",
    "aoi_id": "Short AOI identifier used for filenames and selection.",
}


def run(*, nuts_service: NutsService | None = None, aoi_builder: AoiBuilder | None = None) -> None:
    cfg = load_pipeline_config()

    region_name = cfg.region_name
    aoi_id = cfg.aoi_id

    params = {"region_name": region_name, "aoi_id": aoi_id}
    log_parameters("ExtractAoi", params, PARAMETER_DOCS)

    pipe = ExtractAoiPipeline(
        nuts_service=nuts_service,
        builder=aoi_builder,
    )
    pipe.run(region_name)


def main() -> None:
    # Production CLI path: no injection
    run()


if __name__ == "__main__":
    main()