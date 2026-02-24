# ================================
# Build stage
# ================================
FROM python:3.11-slim AS build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps for GDAL / Rasterio / PROJ
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    proj-bin \
    proj-data \
    build-essential \
    make \
    && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal

WORKDIR /app

# Install project + deps from pyproject.toml
COPY pyproject.toml ./pyproject.toml
COPY src ./src
RUN pip install .

# Copy the rest of the repo (Makefile, config, etc.)
COPY . .

# ================================
# Runtime stage
# ================================
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Runtime libs for GDAL / Rasterio
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    proj-bin \
    proj-data \
    make \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN useradd -ms /bin/bash appuser

WORKDIR /app

# Copy installed site-packages and binaries from build stage
COPY --from=build /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=build /usr/local/bin /usr/local/bin

# Copy project files (Makefile, config, etc.)
COPY --from=build /app /app

# Environment: data lake + repo root + config path (relative to repo root)
ENV DATA_LAKE=/data_lake \
    THESS_GEO_ROOT=/app \
    PIPELINE_CONFIG=config/pipeline.thess.yaml

# Ensure data lake exists and app dir is writable by appuser
RUN mkdir -p ${DATA_LAKE} && \
    chown -R appuser:appuser ${DATA_LAKE} /app

USER appuser

ARG THESS_GEO_ANALYTICS_VERSION="0.1.0"
LABEL org.opencontainers.image.title="Thess Geo Analytics" \
      org.opencontainers.image.version="${THESS_GEO_ANALYTICS_VERSION}"

ENTRYPOINT ["make"]
CMD ["full"]