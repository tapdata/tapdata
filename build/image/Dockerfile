FROM ghcr.io/tapdata/base:0.2

ENV JAVA_TOOL_OPTIONS -Dfile.encoding=UTF8
COPY etc/ /tapdata/apps/etc
COPY connectors/dist.tar.gz /tapdata/apps/connectors/dist.tar.gz
COPY lib/ /tapdata/apps/lib
COPY components/ /tapdata/apps/components
COPY docker-entrypoint.sh /tapdata/apps/start.sh
COPY bin/ /tapdata/apps

RUN apt install supervisor -y

RUN if [[ `uname -p` == "aarch64" ]]; then \
      wget https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-arm64.tar.gz -O /tapdata/apps/components/async-profiler.tar.gz; \
    else \
      wget https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-x64.tar.gz -O /tapdata/apps/components/async-profiler.tar.gz; \
    fi && \
    tar -xzf /tapdata/apps/components/async-profiler.tar.gz -C /tapdata/apps/components/ && \
    rm -f /tapdata/apps/components/async-profiler.tar.gz && \
    mv /tapdata/apps/components/async-profiler-* /tapdata/apps/components/async-profiler

COPY supervisor/ /tapdata/apps/supervisor/

ENV TAPDATA_WORK_DIR /tapdata/apps

ENTRYPOINT ["/tini", "--"]

CMD ["bash", "-c", "/tapdata/apps/start.sh"]