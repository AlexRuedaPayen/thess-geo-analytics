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
The pipeline generates:
</p>

<ul>
<li>NDVI composites</li>
<li>NDVI climatology rasters</li>
<li>NDVI anomaly maps</li>
<li>pixel-level temporal feature stacks</li>
</ul>

<p>
The long-term objective is to detect <b>clusters of pixels with similar temporal behaviour</b>.
These patterns may correspond to:
</p>

<ul>
<li>environmental disturbances</li>
<li>disaster impact</li>
<li>urbanization</li>
<li>vegetation recovery</li>
<li>ecosystem resilience or sensitivity</li>
</ul>

<p>
Future versions will include <b>pixel clustering and anomaly detection models</b>.
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

<p>
Example execution command (PowerShell):
</p>

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

<h3>Run a Decoy Pipeline (Mocked Integration Test)</h3>

<p>
To validate the orchestration logic without downloading or processing real Sentinel-2 rasters,
you can run a <b>decoy pipeline</b> using <b>dummy rasters provided through mocks</b>:
</p>

<pre>
python -m unittest tests.auto.integration.test_WholePipelineTest -v
</pre>

<h3>Run the Full Real Pipeline</h3>

<p>
To execute the complete real pipeline end-to-end:
</p>

<pre>
make full
</pre>

<h3>Run Individual Steps</h3>

<p>
To run individual pipeline steps while respecting dependencies, use:
</p>

<pre>
make help
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
C:\Users\&lt;user&gt;\.wslconfig
</pre>

<p>
Example configuration:
</p>

<pre>
[wsl2]

memory=3GB
processors=2
swap=8GB
localhostForwarding=true
</pre>

<p>
This behaves similarly to <b>Linux cgroups resource limits</b>.
</p>

<p>
Typical Sentinel-2 scene size:
</p>

<pre>
~150 MB
</pre>

<p>
The pipeline opens at most <b>four scenes simultaneously</b>, keeping memory usage around:
</p>

<pre>
~600 MB
</pre>

<p>
Processing relies on <b>window-based raster operations</b> to avoid loading entire rasters into RAM.
</p>

<hr>

<h2>Resolution Downscaling</h2>

<p>
For faster processing the pipeline supports raster downscaling.
</p>

<p>
Example:
</p>

<pre>
10 m Sentinel-2 → 100 m raster
</pre>

<p>
Values are aggregated using the <b>average value within each superpixel</b>.
</p>

<p>
This significantly reduces processing time and disk usage while remaining sufficient 
for <b>regional-scale analysis</b>.
</p>

<hr>

<h2>Pipeline Overview</h2>

<pre>
Sentinel-2 scenes
      ↓
tile aggregation
      ↓
NDVI composites
      ↓
NDVI climatology
      ↓
NDVI anomaly maps
      ↓
pixel time-series feature extraction
</pre>

<p>
Outputs include:
</p>

<ul>
<li>NDVI rasters</li>
<li>climatology rasters</li>
<li>NDVI anomaly rasters</li>
<li>pixel feature stacks</li>
</ul>

<hr>

<h2>Running the Pipeline (Makefile)</h2>

<p>
Available commands:
</p>

<pre>
make extract-aoi
make scene-catalog
make assets-manifest
make timestamps-aggregation
make ndvi-composites
make anomalies
make pixel-features
</pre>

<p>
Run the full pipeline:
</p>

<pre>
make full
</pre>

<hr>

<h2>Example Outputs</h2>

<pre>
ndvi_2023-05_el522.tif
ndvi_anomaly_2023-05_el522.tif
pixel_features_7d_el522.tif
</pre>

<p>
Outputs are written to:
</p>

<pre>
outputs/

    cogs/
        NDVI rasters
        anomaly rasters
        climatology rasters
        feature rasters

    tables/
        diagnostics

    figures/
        PNG previews
</pre>

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

outputs/

tests/
</pre>

<hr>

<h2>Architecture Philosophy</h2>

<h3>geo</h3>

<p>
Core geospatial algorithms:
</p>

<ul>
<li>NDVI computation</li>
<li>cloud masking</li>
<li>AOI masking</li>
<li>window processing</li>
<li>climatology and anomaly computation</li>
<li>pixel feature extraction</li>
</ul>

<h3>builders</h3>

<p>
Heavy raster transformations such as:
</p>

<ul>
<li>NDVI composites</li>
<li>climatology rasters</li>
<li>anomaly maps</li>
<li>feature stacks</li>
</ul>

<h3>pipelines</h3>

<p>
High-level orchestration of the processing workflow.
</p>

<h3>entrypoints</h3>

<p>
Runnable commands responsible for:
</p>

<ul>
<li>loading configuration</li>
<li>initializing pipelines</li>
<li>executing processing steps</li>
</ul>

<h3>services</h3>

<p>
External IO logic:
</p>

<ul>
<li>catalog queries</li>
<li>scene retrieval</li>
<li>asset downloads</li>
</ul>

<hr>

<h2>Requirements</h2>

<ul>
<li>Python 3.11+</li>
<li>Rasterio</li>
<li>NumPy</li>
<li>Pandas</li>
<li>GDAL compatible environment</li>
<li>Docker (recommended)</li>
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
The goal is to build a <b>clean, deployable data pipeline</b> capable of producing
analysis-ready Earth Observation datasets for temporal analysis and machine learning.
</p>
