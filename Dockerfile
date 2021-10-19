ARG PYTHON_VERSION=3.8.12
ARG PYTHON_BASE=buster

FROM python:${PYTHON_VERSION} AS installer

ENV PATH=/root/.local/bin:$PATH

# Copy to tmp folder to don't pollute home dir
RUN mkdir -p /tmp/dist
COPY dist /tmp/dist

RUN ls /tmp/dist
RUN pip install --user --find-links /tmp/dist platform-monitoring

FROM python:${PYTHON_VERSION}-${PYTHON_BASE} AS service

LABEL org.opencontainers.image.source = "https://github.com/neuro-inc/platform-monitoring"

WORKDIR /app

ENV PATH=/root/.local/bin:$PATH

COPY --from=installer /root/.local/ /root/.local/

ENV NP_MONITORING_API_PORT=8080
EXPOSE $NP_MONITORING_API_PORT

CMD platform-monitoring
