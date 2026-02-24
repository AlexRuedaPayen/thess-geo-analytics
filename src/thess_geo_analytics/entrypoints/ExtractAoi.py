from __future__ import annotations

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.pipelines.ExtractAoiPipeline import ExtractAoiPipeline
from thess_geo_analytics.utils.log_parameters import log_parameters


PARAMETER_DOCS = {
    "region_name": "NUTS region name used to derive the AOI polygon.",
    "aoi_id": "Short AOI identifier used for filenames and selection.",
}


def main() -> None:
    cfg = load_pipeline_config()

    region_name = cfg.region_name
    aoi_id = cfg.aoi_id

    # We just pass region_name into the pipeline, but log aoi_id as context
    params = {"region_name": region_name, "aoi_id": aoi_id}

    log_parameters("ExtractAoi", params, PARAMETER_DOCS)

    ExtractAoiPipeline().run(region_name)


if __name__ == "__main__":
    main()