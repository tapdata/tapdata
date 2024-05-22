FROM ghcr.io/tapdata/base:0.1

ENV JAVA_TOOL_OPTIONS -Dfile.encoding=UTF8
COPY etc/ /tapdata/apps/etc
COPY connectors/dist.tar.gz /tapdata/apps/connectors/dist.tar.gz
COPY lib/ /tapdata/apps/lib
COPY components/ /tapdata/apps/components
COPY docker-entrypoint.sh /tapdata/docker-entrypoint.sh

ARG TARGETPLATFORM

COPY ${TARGETPLATFORM:-linux/amd64}/tapdata /tapdata/apps/tapdata

COPY application.yml /tapdata/apps/application.yml

ENV TAPDATA_WORK_DIR /tapdata/apps

ENTRYPOINT ["/tini", "--"]

CMD ["bash", "-c", "/tapdata/docker-entrypoint.sh"]
