FROM ubuntu:22.04
RUN apt update -y && apt install curl -y

ENV JAVA_HOME=/opt/jdk1.8.0_401
COPY jdk1.8.0_401 $JAVA_HOME
RUN ln -s $JAVA_HOME/bin/java /usr/local/bin/java

COPY mongod /usr/local/bin/
RUN chmod +x /usr/local/bin/mongod

COPY mongo /usr/local/bin/
RUN chmod +x /usr/local/bin/mongo

COPY tini /
RUN chmod +x /tini

ENV TAPDATA_WORK_DIR /tapdata/apps

ENTRYPOINT ["/tini", "--"]

CMD ["bash", "-c", "/tapdata/docker-entrypoint.sh"]
