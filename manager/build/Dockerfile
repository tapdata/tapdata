
FROM tapdata-docker.pkg.coding.net/wendangshujuku/images/tapdata-basic-java-node:18.04-8-16
ENV LC_ALL="en_US.UTF-8" LANG="en_US.UTF-8"
WORKDIR /opt

COPY tapdata-tm--version- \
     gateway \
     tapdata-v1.25.2-0-gf71f28413-linux-x64.tar.gz \
     start.sh \
     /opt/

RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8 && \
    tar -zxf tapdata-v1.25.2-0-gf71f28413-linux-x64.tar.gz

EXPOSE 3000
EXPOSE 3001
EXPOSE 3002
EXPOSE 3003
EXPOSE 3080

CMD ["start.sh"]

