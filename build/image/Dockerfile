FROM ghcr.io/tapdata/base:0.2

ENV JAVA_TOOL_OPTIONS -Dfile.encoding=UTF8

# 安装Java 17并设置为默认版本
RUN mkdir -p /usr/java && \
    wget 'http://192.168.1.184:5244/d/tools/java-17.tar.gz?sign=tKZO5QQTne90dRexZUoio6WNygJ_di_CScTa3wFuInY=:0' -O java-17.tar.gz && \
    tar -xzf java-17.tar.gz -C /usr/java && \
    rm -f java-17.tar.gz && \
    update-alternatives --install "/usr/bin/java" "java" "/usr/java/jdk-17.0.12/bin/java" 2 && \
    update-alternatives --install "/usr/bin/javac" "javac" "/usr/java/jdk-17.0.12/bin/javac" 2 && \
    update-alternatives --install "/usr/bin/jar" "jar" "/usr/java/jdk-17.0.12/bin/jar" 2 && \
    update-alternatives --set java /usr/java/jdk-17.0.12/bin/java && \
    update-alternatives --set javac /usr/java/jdk-17.0.12/bin/javac && \
    update-alternatives --set jar /usr/java/jdk-17.0.12/bin/jar && \
    java -version

COPY etc/ /tapdata/apps/etc
COPY connectors/dist.tar.gz /tapdata/apps/connectors/dist.tar.gz
COPY lib/ /tapdata/apps/lib
COPY components/ /tapdata/apps/components
COPY docker-entrypoint.sh /tapdata/apps/start.sh
COPY bin/ /tapdata/apps

RUN apt update && apt install supervisor --fix-missing -y

# 使用已经下载好的async-profiler文件
COPY components/async-profiler/ /tapdata/apps/components/async-profiler/

COPY supervisor/ /tapdata/apps/supervisor/

ENV TAPDATA_WORK_DIR /tapdata/apps

ENTRYPOINT ["/tini", "--"]

CMD ["bash", "-c", "/tapdata/apps/start.sh"]