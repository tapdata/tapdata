FROM ghcr.io/tapdata/tapdata/tldp/runtime-ee:0.5

ENV JAVA_TOOL_OPTIONS -Dfile.encoding=UTF8
COPY connectors/dist.tar.gz /tapdata/apps/connectors/dist.tar.gz
COPY manager/ /tapdata/apps/manager/
COPY iengine/ /tapdata/apps/iengine/
COPY pdk.jar /tapdata/apps/lib/pdk.jar
COPY components/ /tapdata/apps/components/
COPY docker-entrypoint.sh /tapdata/docker-entrypoint.sh
RUN chmod +x /tapdata/docker-entrypoint.sh && mkdir -p /tapdata/data/db/

# Add tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ENV TAPDATA_HOME /tapdata/apps

ENTRYPOINT ["/tini", "--"]

CMD ["bash", "-c", "/tapdata/docker-entrypoint.sh"]
