FROM ghcr.io/tapdata/base:0.2

ENV JAVA_TOOL_OPTIONS -Dfile.encoding=UTF8

# 设置 DEBIAN_FRONTEND 为 noninteractive 以避免交互式提示
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install wget supervisor python3-pip pipx tzdata --fix-missing -y

# 安装Java 17并设置为默认版本
RUN mkdir -p /usr/java

# 下载适合x86_64架构的JDK
RUN if [ "$(uname -m)" = "x86_64" ]; then \
        wget 'http://192.168.1.184:5244/d/tools/java-17.tar.gz?sign=tKZO5QQTne90dRexZUoio6WNygJ_di_CScTa3wFuInY=:0' -O java-17.tar.gz; \
    fi

# 下载适合aarch64架构的JDK
RUN if [ "$(uname -m)" = "aarch64" ]; then \
        wget 'http://192.168.1.184:5244/d/tools/jdk-17.0.15_linux-aarch64_bin.tar.gz?sign=zaNY1HBN8lEDUjMyQPkLfwGhEPpw0-WDuKFGZe-nVOE=:0' -O java-17.tar.gz; \
    fi

# 解压JDK
RUN tar -xzf java-17.tar.gz -C /usr/java && \
    rm -f java-17.tar.gz

# 为x86_64架构配置java环境
RUN if [ "$(uname -m)" = "x86_64" ]; then \
      update-alternatives --install "/usr/bin/java" "java" "/usr/java/jdk-17.0.12/bin/java" 2 && \
      update-alternatives --install "/usr/bin/javac" "javac" "/usr/java/jdk-17.0.12/bin/javac" 2 && \
      update-alternatives --install "/usr/bin/jar" "jar" "/usr/java/jdk-17.0.12/bin/jar" 2 && \
      update-alternatives --set java /usr/java/jdk-17.0.12/bin/java && \
      update-alternatives --set javac /usr/java/jdk-17.0.12/bin/javac && \
      update-alternatives --set jar /usr/java/jdk-17.0.12/bin/jar; \
    fi

# 为aarch64架构配置java环境
RUN if [ "$(uname -m)" = "aarch64" ]; then \
      update-alternatives --install "/usr/bin/java" "java" "/usr/java/jdk-17.0.15/bin/java" 2 && \
      update-alternatives --install "/usr/bin/javac" "javac" "/usr/java/jdk-17.0.15/bin/javac" 2 && \
      update-alternatives --install "/usr/bin/jar" "jar" "/usr/java/jdk-17.0.15/bin/jar" 2 && \
      update-alternatives --set java /usr/java/jdk-17.0.15/bin/java && \
      update-alternatives --set javac /usr/java/jdk-17.0.15/bin/javac && \
      update-alternatives --set jar /usr/java/jdk-17.0.15/bin/jar; \
    fi

# 验证Java版本
RUN java -version

COPY etc/ /tapdata/apps/etc
COPY connectors/dist.tar.gz /tapdata/apps/connectors/dist.tar.gz
COPY lib/ /tapdata/apps/lib
COPY components/ /tapdata/apps/components
COPY docker-entrypoint.sh /tapdata/apps/start.sh
COPY bin/ /tapdata/apps

RUN pipx install tapflow

ENV PATH="/root/.local/bin:$PATH"

# 使用已经下载好的async-profiler文件
COPY components/async-profiler/ /tapdata/apps/components/async-profiler/

COPY supervisor/ /tapdata/apps/supervisor/

ENV TAPDATA_WORK_DIR /tapdata/apps

ENTRYPOINT ["/tini", "--"]

CMD ["bash", "-c", "/tapdata/apps/start.sh"]