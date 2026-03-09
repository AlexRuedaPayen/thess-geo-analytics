<h1>Thess Geo Analytics</h1>

<p>
Sentinel-2 raster processing pipeline for <b>NDVI time-series analysis</b> and 
<b>pixel-level feature extraction</b>.
</p>

<p>
Developed by <b>Alexandre Rueda Payen</b>.
</p>

<hr>

<h2>Project Goal</h2>

<p>
This repository implements a <b>geospatial Earth Observation pipeline</b> that transforms
Sentinel-2 imagery into <b>analysis-ready datasets</b> for temporal analysis and machine learning.
</p>

<p>
The pipeline produces three main types of outputs:
</p>

<ul>
<li>NDVI climatology rasters</li>
<li>NDVI anomaly time series</li>
<li>pixel-level temporal feature stacks</li>
</ul>

<p>
The long-term objective is to detect <b>clusters of pixels with similar temporal behaviour</b>.
These clusters may correspond to:
</p>

<ul>
<li>environmental disturbances</li>
<li>disaster impact</li>
<li>urbanization</li>
<li>vegetation recovery</li>
<li>ecosystem resilience or sensitivity</li>
</ul>

<p>
The final goal is to use these features as input to <b>machine learning models</b>
capable of detecting spatial patterns and changes in land dynamics.
</p>

<hr>

<h2>Study Region</h2>

<p>
The current implementation focuses on:
</p>

<ul>
<li><b>Thessaloniki (Greece)</b></li>
</ul>

<p>
Future extensions are planned for:
</p>

<ul>
<li><b>Halkidiki</b></li>
</ul>

<p>
Current constraints:
</p>

<ul>
<li>The pipeline currently accepts <b>one NUTS level 3 region</b> as input</li>
<li>Thessaloniki AOI size: <b>~X km²</b></li>
</ul>

<hr>

<h2>Quick Start (Recommended: Docker)</h2>

<p>
The pipeline is easiest to run using <b>Docker</b>.
This avoids dependency issues related to GDAL, Rasterio, and other geospatial libraries.
</p>

<h3>1. Build the Docker Image</h3>

<pre>
docker build -t thess-geo-analytics:0.3.1 .
</pre>

<p>
Typical image size:
</p>

<pre>
~2.5 GB
</pre>

<h3>2. Run the Pipeline</h3>

<pre>
docker run -it --rm `
  -v "C:\...\DATA_LAKE:/data_lake" `
  -v "C:\...\aoi:/app/aoi" `
  -v "C:\...\outputs:/app/outputs" `
  -v "C:\...\config:/app/config:ro" `
  -v "C:\...\ .env:/app/.env:ro" `
  --env-file "C:\...\ .env" `
  -e DATA_LAKE=/data_lake `
  -e PIPELINE_CONFIG=config/pipeline.thess.yaml `
  -e THESS_GEO_ROOT=/app `
  thess-geo-analytics:0.3.1
</pre>

<hr>

<h2>Running the Pipeline</h2>

<h3>Run a Decoy Pipeline (Mocked Integration Test) (~3 minutes)</h3>

<pre>
python -m unittest tests.auto.integration.test_WholePipelineTest -v
</pre>

<h3>Run the Full Pipeline (~4 hours)</h3>

<pre>
make full
</pre>

<h3>Run Individual Steps</h3>

<pre>
make help
</pre>

<hr>

<h2>Visualizing Results</h2>

<p>
The repository includes a visualization utility that generates preview images
from the produced rasters.
</p>

<pre>
make visualize
</pre>

<p>
This reads rasters from:
</p>

<pre>
outputs/cogs/
</pre>

<p>
and exports PNG previews to:
</p>

<pre>
docs/
</pre>

<p>
This tool is useful for quickly inspecting:
</p>

<ul>
<li>NDVI composites</li>
<li>NDVI anomalies</li>
<li>NDVI climatology maps</li>
<li>pixel feature rasters</li>
</ul>

<hr>

<h2>Runtime Characteristics</h2>

<ul>
<li>Typical runtime: <b>~4 hours</b></li>
<li>Docker image size: <b>~2.5 GB</b></li>
<li>Pipeline disk usage: <b>~20 GB</b></li>
<li>Recommended free disk space: <b>≥ 50 GB</b></li>
</ul>

<hr>

<h2>Memory Management</h2>

<p>
Memory usage is limited using a WSL2 configuration file:
</p>

<pre>
[wsl2]
memory=3GB
processors=2
swap=8GB
</pre>

<p>
Typical Sentinel-2 scene size:
</p>

<pre>
~150 MB
</pre>

<p>
The pipeline never loads more than <b>four scenes simultaneously</b>,
keeping memory usage below roughly:
</p>

<pre>
~600 MB
</pre>

<hr>

<h2>Pipeline Parameters</h2>

<p>
The experiment described in this repository was run using the following configuration:
</p>

<pre>
mode: "deep"
debug: false

region: "Thessaloniki"
aoi_id: "el522"

pipeline:
  date_start: "2023-01-01"

raster:
  resolution: 20

scene_catalog:
  cloud_cover_max: 20.0
  max_items: 3000
  full_cover_threshold: 0.95
  n_anchors: 18
  window_days: 21
  collection: "sentinel-2-l2a"

ndvi_composites:
  min_scenes_per_month: 2
  fallback_to_quarterly: true
  strategy: "monthly"
  cloud_masking: true
</pre>

<p>
These parameters resulted in:
</p>

<ul>
<li><b>14 Sentinel-2 scenes</b> downloaded</li>
<li>Scenes filtered by <b>cloud cover and AOI completeness</b></li>
<li>Images <b>downscaled to 20 m resolution</b></li>
<li><b>11 quarterly NDVI rasters</b> generated</li>
</ul>

<p>
Although the catalog attempted to retrieve <b>18 equidistant timestamps</b>,
several scenes were discarded due to incomplete coverage or excessive cloud cover.
</p>

<hr>

<h2>Example Results and Visual Analysis</h2>

<p>
The following figures illustrate the intermediate and final outputs
produced by the pipeline.
</p>

<hr>

<h2>NDVI Climatology Maps</h2>

<h3>Q1</h3>
<img src="docs/ndvi_climatology/ndvi_climatology_median_Q1_el522.png" width="700">

<p>
Lorem ipsum — analysis of winter vegetation baseline and spatial distribution.
</p>

<h3>Q2</h3>
<img src="docs/ndvi_climatology/ndvi_climatology_median_Q2_el522.png" width="700">

<p>
Lorem ipsum — discussion of peak vegetation season.
</p>

<h3>Q3</h3>
<img src="docs/ndvi_climatology/ndvi_climatology_median_Q3_el522.png" width="700">

<p>
Lorem ipsum — late summer vegetation patterns.
</p>

<h3>Q4</h3>
<img src="docs/ndvi_climatology/ndvi_climatology_median_Q4_el522.png" width="700">

<p>
Lorem ipsum — seasonal vegetation decline.
</p>

<hr>

<h2>NDVI Anomaly Time Series</h2>

<p>
NDVI anomaly is defined as:
</p>

<pre>
NDVI anomaly = NDVI_observed − NDVI_climatology
</pre>

<h3>Example: 2023 Q1</h3>

<img src="docs/ndvi_anomaly/ndvi_anomaly_2023-Q1_el522.png" width="700">

<p>
Lorem ipsum — interpretation of anomaly signals.
</p>

<h3>Example: 2024 Q3</h3>

<img src="docs/ndvi_anomaly/ndvi_anomaly_2024-Q3_el522.png" width="700">

<p>
Lorem ipsum — discussion of vegetation deviations.
</p>

<hr>

<h2>Pixel Feature Encoding (7D)</h2>

<p>
The anomaly time series is encoded into a <b>7-dimensional feature vector</b>
for each pixel.
</p>

<ul>
<li>trend slope</li>
<li>seasonal variability</li>
<li>minimum anomaly</li>
<li>recovery ratio</li>
<li>anomaly persistence</li>
<li>NDVI variance</li>
<li>NDVI skewness</li>
</ul>

<h3>Band 1 — Trend</h3>
<img src="docs/pixel_features/pixel_features_7d_el522_band_1.png" width="700">

<h3>Band 2 — Seasonal Variability</h3>
<img src="docs/pixel_features/pixel_features_7d_el522_band_2.png" width="700">

<h3>Band 3 — Minimum Anomaly</h3>
<img src="docs/pixel_features/pixel_features_7d_el522_band_3.png" width="700">

<h3>Band 4 — Recovery Ratio</h3>
<img src="docs/pixel_features/pixel_features_7d_el522_band_4.png" width="700">

<h3>Band 5 — Anomaly Persistence</h3>
<img src="docs/pixel_features/pixel_features_7d_el522_band_5.png" width="700">

<h3>Band 6 — NDVI Variance</h3>
<img src="docs/pixel_features/pixel_features_7d_el522_band_6.png" width="700">

<h3>Band 7 — NDVI Skewness</h3>
<img src="docs/pixel_features/pixel_features_7d_el522_band_7.png" width="700">

<hr>

<h2>Repository Architecture</h2>

<pre>
thess-geo-analytics/

config/
    pipeline.thess.yaml

src/thess_geo_analytics/
    entrypoints/
    pipelines/
    builders/
    geo/
    services/
    utils/
    tools/

docs/
tests/
</pre>

<hr>

<h2>Purpose of the Project</h2>

<p>
This repository demonstrates:
</p>

<ul>
<li>Earth Observation raster engineering</li>
<li>Sentinel-2 processing pipelines</li>
<li>geospatial data engineering</li>
<li>reproducible EO workflows</li>
</ul>

<p>
The goal is to build a <b>clean, deployable geospatial data pipeline</b>
capable of producing analysis-ready Earth Observation datasets
for temporal analysis and machine learning.
</p>