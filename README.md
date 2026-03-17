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

Recommended version : 2.0.0 (see latest release)
</p>

<h3>1. Build the Docker Image</h3>

<pre>
docker build -t thess-geo-analytics:2.0.0 .
</pre>

<p>
Typical image size:
</p>

<pre>
~2.5 GB
</pre>

<h3>2. Prepare the <code>.env</code> File</h3>

<p>
The pipeline requires credentials for the satellite data services used during scene retrieval.
Create a <code>.env</code> file at the root of the project and store the required secrets there.
</p>

<p>
Example:
</p>

<pre>
SH_CLIENT_ID=xxxx
SH_CLIENT_SECRET=xxxx

CDSE_USERNAME=xxxx
CDSE_PASSWORD=xxxx
</pre>

<p>
Where:
</p>

<ul>
<li><b>SH_CLIENT_ID / SH_CLIENT_SECRET</b> — Sentinel Hub credentials</li>
<li><b>CDSE_USERNAME / CDSE_PASSWORD</b> — Copernicus Data Space Ecosystem credentials</li>
</ul>

<p>
When running the container, this file is mounted read-only and also passed through
<code>--env-file</code>.
</p>

<h3>3. Run the Pipeline</h3>

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
  thess-geo-analytics:2.0.0
</pre>

<p>
Mounted directories typically include:
</p>

<ul>
<li><b>DATA_LAKE</b> — raw Sentinel-2 assets and intermediate rasters</li>
<li><b>aoi</b> — AOI geometries and masks</li>
<li><b>outputs</b> — generated rasters, tables, and preview figures</li>
<li><b>config</b> — pipeline configuration files</li>
<li><b>.env</b> — secrets and provider credentials</li>
</ul>

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

<p>
Processing relies on block-wise raster operations to avoid loading entire rasters into RAM.
</p>

<hr>

<h2>Normalized Difference Vegetation Index (NDVI)</h2>

<p>
NDVI is one of the most widely used vegetation indicators in Earth Observation.
It compares near-infrared reflectance and red reflectance to estimate vegetation
density and vegetation activity.
</p>

<p align="center">
<b>NDVI = (NIR − RED) / (NIR + RED)</b>
</p>

<p>
For Sentinel-2 imagery:
</p>

<ul>
<li><b>B08</b> — Near Infrared (NIR)</li>
<li><b>B04</b> — Red</li>
</ul>

<h3>Observed NDVI Limitations</h3>

<p>
During analysis it was observed that some non-vegetated surfaces such as water bodies
and dense urban areas occasionally exhibited slightly positive NDVI values.
</p>

<p>
Possible causes include:
</p>

<ul>
<li>mixed pixels at coarse spatial resolution</li>
<li>surface reflectance noise</li>
<li>atmospheric correction artefacts</li>
<li>subpixel vegetation or algae presence</li>
</ul>

<p>
To validate the NDVI computation itself, a synthetic reconstruction test was implemented.
</p>

<pre>
tests.auto.unit.test_NdviReconstructionFromSyntheticBandsTest
</pre>

<p>
This test reconstructs NDVI values from synthetic red and NIR bands to ensure
the NDVI processor correctly computes the index.
</p>

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
These parameters control:
</p>

<ul>
<li>scene filtering thresholds</li>
<li>temporal sampling density</li>
<li>cloud filtering</li>
<li>NDVI compositing strategy</li>
<li>output raster resolution</li>
</ul>

<p>
These parameters resulted in:
</p>

<ul>
<li><b>14 Sentinel-2 scenes</b> downloaded</li>
<li>Scenes filtered by <b>cloud cover and AOI completeness</b></li>
<li>Images <b>downscaled to 20 m resolution</b></li>
<li><b>11 quarterly NDVI rasters</b> generated</li>
</ul>

<hr>

<h2>Example Results and Visual Analysis</h2>

<p>
The following figures illustrate the intermediate and final outputs produced by the pipeline.
</p>

<h2>NDVI Climatology Maps</h2>

<p>
These maps represent the <b>median NDVI value</b> observed for each season
(quarter of the year). The median is used instead of the mean because it is
more robust to outliers such as residual cloud contamination or atmospheric noise.
</p>

<h3>Q1</h3>
<img src="docs/ndvi_climatology/ndvi_climatology_median_Q1_el522.png" width="700">

<p>
We can see the body of water (Limni Volvi and Periferiaki zoni B) having an NDVI close to 0.00.
Chortiatis mountain area and the Axios Delta show relatively low values, while some agricultural
areas around Perea appear slightly higher.
</p>

<h3>Q2</h3>
<img src="docs/ndvi_climatology/ndvi_climatology_median_Q2_el522.png" width="700">

<p>
Vegetation remains relatively low around the Axios Delta but increases strongly in Chortiatis.
This is consistent with spring conditions.
</p>

<h3>Q3</h3>
<img src="docs/ndvi_climatology/ndvi_climatology_median_Q3_el522.png" width="700">

<p>
The dry season begins. The Perea subregion becomes substantially less vegetated,
while Chortiatis remains high.
</p>

<h3>Q4</h3>
<img src="docs/ndvi_climatology/ndvi_climatology_median_Q4_el522.png" width="700">

<p>
Vegetation decreases across most of the AOI as winter approaches.
</p>

<hr>

<h2>NDVI Anomaly Time Series</h2>

<p align="center">
<b>NDVI<sub>anomaly</sub> = NDVI<sub>observed</sub> − NDVI<sub>climatology</sub></b>
</p>

<img src="docs/ndvi_anomaly/ndvi_anomaly_2023-Q1_el522.png" width="700">

<img src="docs/ndvi_anomaly/ndvi_anomaly_2024-Q3_el522.png" width="700">

<hr>

<h2>Pixel Feature Encoding (7D)</h2>

<ul>
<li><b>Trend slope</b></li>
<li><b>Seasonal variability</b></li>
<li><b>Minimum anomaly</b></li>
<li><b>Recovery ratio</b></li>
<li><b>Anomaly persistence</b></li>
<li><b>NDVI variance</b></li>
<li><b>NDVI skewness</b></li>
</ul>

<img src="docs/pixel_features/pixel_features_7d_el522_band_1.png" width="700">
<img src="docs/pixel_features/pixel_features_7d_el522_band_2.png" width="700">
<img src="docs/pixel_features/pixel_features_7d_el522_band_3.png" width="700">
<img src="docs/pixel_features/pixel_features_7d_el522_band_4.png" width="700">
<img src="docs/pixel_features/pixel_features_7d_el522_band_5.png" width="700">
<img src="docs/pixel_features/pixel_features_7d_el522_band_6.png" width="700">
<img src="docs/pixel_features/pixel_features_7d_el522_band_7.png" width="700">

<hr>

<h2>Repository Architecture</h2>

<p>
The repository is structured to separate algorithmic logic,
pipeline orchestration, and infrastructure components.
</p>

<pre>
src/thess_geo_analytics/

entrypoints/
pipelines/
builders/
geo/
services/
utils/
tools/
</pre>

<ul>
<li><b>entrypoints</b> — CLI entry scripts used by the Makefile</li>
<li><b>pipelines</b> — orchestration of processing stages</li>
<li><b>builders</b> — heavy raster transformations</li>
<li><b>geo</b> — core geospatial algorithms</li>
<li><b>services</b> — interaction with external APIs</li>
<li><b>utils</b> — shared helper utilities</li>
<li><b>tools</b> — debugging and visualization utilities</li>
</ul>

<hr>

<h2>Technical Documentation (Wiki)</h2>

<p>
Detailed explanations of algorithms and design decisions are available in the Wiki.
</p>

<ul>
<li><a href="https://github.com/AlexRuedaPayen/thess-geo-analytics/wiki/AOI-Raster-Window">AOI Raster Window</a></li>
<li><a href="https://github.com/AlexRuedaPayen/thess-geo-analytics/wiki/Cloud-Masker">Cloud Masker</a></li>
<li><a href="https://github.com/AlexRuedaPayen/thess-geo-analytics/wiki/NDVI-Aggregated-Composite-Builder">NDVI Aggregated Composite Builder</a></li>
<li><a href="https://github.com/AlexRuedaPayen/thess-geo-analytics/wiki/Ndvi-Processor">NDVI Processor</a></li>
<li><a href="https://github.com/AlexRuedaPayen/thess-geo-analytics/wiki/Pipeline-Nvdi-Anomaly-Maps">NDVI Anomaly Pipeline</a></li>
<li><a href="https://github.com/AlexRuedaPayen/thess-geo-analytics/wiki/Pipeline-Pixel-Features">Pixel Feature Pipeline</a></li>
<li><a href="https://github.com/AlexRuedaPayen/thess-geo-analytics/wiki/Pixel-Feature-Extractor">Pixel Feature Extractor</a></li>
<li><a href="https://github.com/AlexRuedaPayen/thess-geo-analytics/wiki/Test-Whole-Pipeline-CI">Pixel Feature Extractor</a></li>
</ul>

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



<h2> Licence </h2>

<p>
This project is licensed under the MIT License.
You are free to use it commercially, but must include attribution.
</p>