ARG PY_VERSION=3.11.4

FROM python:${PY_VERSION}-slim-bookworm AS requirements

ENV PATH=/root/.local/bin:$PATH

# Copy to tmp folder to don't pollute home dir
RUN mkdir -p /tmp/dist
COPY dist /tmp/dist

RUN ls /tmp/dist
RUN pip install --user --find-links /tmp/dist platform-monitoring

FROM python:${PY_VERSION}-slim-bookworm AS service

LABEL org.opencontainers.image.source = "https://github.com/neuro-inc/platform-monitoring"

WORKDIR /app

ENV PATH=/root/.local/bin:$PATH

COPY --from=requirements /root/.local/ /root/.local/

ENV NP_MONITORING_API_PORT=8080
EXPOSE $NP_MONITORING_API_PORT

CMD [ "platform-monitoring-api" ]
