ARG PY_VERSION=3.14.10

FROM python:${PY_VERSION}-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && python -c "from importlib.metadata import version; assert tuple(map(int, version('pip').split('.'))) >= (26, 2, 2)"

ENV PATH=/root/.local/bin:$PATH

WORKDIR /tmp
COPY requirements.txt /tmp/
RUN pip install --user --no-cache-dir -r requirements.txt

COPY dist /tmp/dist/
RUN pip install --user --no-cache-dir --find-links /tmp/dist platform-monitoring

FROM python:${PY_VERSION}-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && python -c "from importlib.metadata import version; assert tuple(map(int, version('pip').split('.'))) >= (26, 2, 2)"
LABEL org.opencontainers.image.source="https://github.com/neuro-inc/platform-monitoring"

# Name of your service (folder under /home)
ARG SERVICE_NAME="platform-monitoring-api"
ARG SERVICE_UID=1001
ARG SERVICE_GID=1001

RUN addgroup --gid $SERVICE_GID $SERVICE_NAME && \
    adduser --uid $SERVICE_UID --gid $SERVICE_GID \
    --home /home/$SERVICE_NAME --shell /bin/false \
    --disabled-password --gecos "" $SERVICE_NAME

# Tell Python where the "user" site is
ENV HOME=/home/${SERVICE_NAME}
ENV PYTHONUSERBASE=/home/${SERVICE_NAME}/.local
ENV PATH=/home/${SERVICE_NAME}/.local/bin:$PATH

# Copy everything from the builder’s user‐site into your service’s user‐site
COPY --from=builder --chown=$SERVICE_NAME:$SERVICE_GID /root/.local /home/${SERVICE_NAME}/.local

WORKDIR /app

ENV NP_MONITORING_API_PORT=8080
EXPOSE $NP_MONITORING_API_PORT


CMD [ "platform-monitoring-api" ]
