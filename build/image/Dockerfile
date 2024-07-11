FROM harbor.internal.tapdata.io/tapdata/base:0.2

ENV JAVA_TOOL_OPTIONS -Dfile.encoding=UTF8
COPY etc/ /tapdata/apps/etc
COPY connectors/dist.tar.gz /tapdata/apps/connectors/dist.tar.gz
COPY lib/ /tapdata/apps/lib
COPY components/ /tapdata/apps/components
COPY docker-entrypoint.sh /tapdata/apps/start.sh
COPY bin/ /tapdata/apps

RUN apt install supervisor -y

RUN apt install wget -y && \
    if [[ `uname -p` == "aarch64" ]]; then \
      wget http://58.251.34.123:5244/d/tools/async-profiler-3.0-linux-arm64.tar.gz?sign=8jGp5W4yfHMMUAWwyctj5KMzyj8NHpC817hOycbSa0A=:0 -O /tapdata/apps/components/async-profiler.tar.gz; \
    else \
      wget http://58.251.34.123:5244/d/tools/async-profiler-3.0-linux-x64.tar.gz?sign=lwX2LlQGKO0Ph3M9AuygTGki9R_d76zTeRC5x_Gzzs8=:0 -O /tapdata/apps/components/async-profiler.tar.gz; \
    fi && \
    tar -xzf /tapdata/apps/components/async-profiler.tar.gz -C /tapdata/apps/components/ && \
    rm -f /tapdata/apps/components/async-profiler.tar.gz && \
    mv /tapdata/apps/components/async-profiler-* /tapdata/apps/components/async-profiler

COPY supervisor/ /tapdata/apps/supervisor/

ENV TAPDATA_WORK_DIR /tapdata/apps

ENTRYPOINT ["/tini", "--"]

CMD ["bash", "-c", "/tapdata/apps/start.sh"]