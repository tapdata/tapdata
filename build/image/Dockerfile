FROM ghcr.io/tapdata/base:0.1

ENV JAVA_TOOL_OPTIONS -Dfile.encoding=UTF8
COPY etc/ /tapdata/apps/etc
COPY connectors/dist.tar.gz /tapdata/apps/connectors/dist.tar.gz
COPY lib/ /tapdata/apps/lib
COPY components/ /tapdata/apps/components
COPY docker-entrypoint.sh /tapdata/apps/start.sh
COPY bin/ /tapdata/apps

RUN apt install supervisor -y

COPY supervisor/ /tapdata/apps/supervisor/

ENV TAPDATA_WORK_DIR /tapdata/apps

ENTRYPOINT ["/tini", "--"]

CMD ["bash", "-c", "/tapdata/apps/start.sh"]