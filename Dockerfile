# syntax=docker/dockerfile:1.6
# Stage 0: Dependency Cacher
FROM alpine:latest AS dependency-cacher
WORKDIR /app
# Copy tapdata and tapdata-connectors structure
COPY tapdata/ tapdata/
COPY tapdata-connectors/ tapdata-connectors/

# Keep only pom.xml files to optimize cache invalidation
RUN find . -type f -not -name "pom.xml" -delete && find . -type d -empty -delete

# Stage 1: Builder
FROM maven:3.8-openjdk-17 AS builder

WORKDIR /app

ARG TAP_CONNECTORS="mongodb"
ARG MAVEN_OPTIONS="-Xms4g -Xmx4g -XX:MaxMetaspaceSize=2g"

# Configure Aliyun Maven Mirror
RUN echo '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" \
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
  xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd"> \
  <mirrors> \
    <mirror> \
      <id>aliyunmaven</id> \
      <mirrorOf>*,!tapdata-wendangshujuku-mongo,!nexus-releases,!nexus-snapshots,!tapdata-tapdata-maven</mirrorOf> \
      <name>Aliyun Public Repository</name> \
      <url>https://maven.aliyun.com/repository/public</url> \
    </mirror> \
  </mirrors> \
  <profiles> \
    <profile> \
          <id>fast</id> \
          <properties> \
            <maven.artifact.threads>8</maven.artifact.threads> \
          </properties> \
    </profile> \
    <profile> \
        <id>aliyun-first</id> \
        <repositories> \
            <repository> \
                <id>central</id> \
                <url>https://maven.aliyun.com/repository/public</url> \
                <releases><enabled>true</enabled></releases> \
                <snapshots><enabled>false</enabled></snapshots> \
            </repository> \
            <repository> \
                <id>tapdata-wendangshujuku-mongo</id> \
                <name>mongo</name> \
                <url>https://tapdata-maven.pkg.coding.net/repository/wendangshujuku/mongo/</url> \
                <releases><enabled>true</enabled></releases> \
                <snapshots><enabled>true</enabled></snapshots> \
            </repository> \
            <repository> \
                <id>nexus-releases</id> \
                <name>nexus-maven-release</name> \
                <url>https://nexus.tapdata.net/repository/maven-releases/</url> \
                <releases><enabled>true</enabled></releases> \
                <snapshots><enabled>false</enabled></snapshots> \
            </repository> \
            <repository> \
                <id>nexus-snapshots</id> \
                <name>nexus-maven-snapshot</name> \
                <url>https://nexus.tapdata.net/repository/maven-snapshots/</url> \
                <releases><enabled>false</enabled></releases> \
                <snapshots><enabled>true</enabled></snapshots> \
            </repository> \
            <repository> \
                <id>tapdata-tapdata-maven</id> \
                <name>maven</name> \
                <url>https://tapdata-maven.pkg.coding.net/repository/tapdata/maven/</url> \
                <releases><enabled>true</enabled></releases> \
                <snapshots><enabled>true</enabled></snapshots> \
            </repository> \
        </repositories> \
        <pluginRepositories> \
            <pluginRepository> \
                <id>central</id> \
                <url>https://maven.aliyun.com/repository/public</url> \
                <releases><enabled>true</enabled></releases> \
                <snapshots><enabled>false</enabled></snapshots> \
            </pluginRepository> \
        </pluginRepositories> \
    </profile> \
  </profiles> \
  <activeProfiles> \
    <activeProfile>fast</activeProfile> \
    <activeProfile>aliyun-first</activeProfile> \
  </activeProfiles> \
</settings>' > /usr/share/maven/conf/settings.xml

# Copy only pom.xml structure from the cacher stage
COPY --from=dependency-cacher /app /app

# Remove repositories from root pom.xml to force use of settings.xml order
RUN sed -i '/<repositories>/,/<\/repositories>/d' tapdata/pom.xml || true
RUN sed -i '/<repositories>/,/<\/repositories>/d' tapdata-connectors/pom.xml || true

# Download dependencies (Cached Layer)
WORKDIR /app/tapdata

WORKDIR /app/tapdata-connectors
RUN echo "Skipping connectors dependency prefetch in core builder stage"

# Copy the entire project (source code) - This layer changes frequently
WORKDIR /app
COPY tapdata/ tapdata/
COPY tapdata-connectors/ tapdata-connectors/

# Build Tapdata (Manager, Engine, CLI)
WORKDIR /app/tapdata
# Use -pl !tapdata-test to build all modules except test
RUN --mount=type=cache,target=/root/.m2,sharing=locked MAVEN_OPTS="$MAVEN_OPTIONS" mvn clean install -pl !tapdata-test -am -P idaas,not_encrypt -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip -Dspotbugs.skip -DskipITs -T 1C -B

FROM builder AS engine-builder
WORKDIR /app/tapdata-connectors
RUN --mount=type=cache,target=/root/.m2,sharing=locked MAVEN_OPTS="$MAVEN_OPTIONS" mvn clean install -pl connectors-common -am -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip -Dspotbugs.skip -DskipITs -T 1C -B
RUN --mount=type=cache,target=/root/.m2,sharing=locked <<'EOF'
set -e

MODULES=""
CONNECTOR_IDS="${TAP_CONNECTORS:-mongodb}"
if [ -z "$CONNECTOR_IDS" ]; then CONNECTOR_IDS="mongodb"; fi

IFS=','; for raw in $CONNECTOR_IDS; do
  id="$(printf '%s' "$raw" | tr -d '[:space:]')"
  [ -z "$id" ] && continue

  if [ -f "connectors/${id}-connector/pom.xml" ]; then
    module="connectors/${id}-connector"
  elif [ -f "connectors-javascript/${id}-connector/pom.xml" ]; then
    module="connectors-javascript/${id}-connector"
  else
    echo "Skip unknown connector id: ${id}"
    continue
  fi

  if [ -z "$MODULES" ]; then MODULES="$module"; else MODULES="${MODULES},${module}"; fi
done

if [ -n "$MODULES" ]; then
  MAVEN_OPTS="$MAVEN_OPTIONS" mvn clean install -pl "$MODULES" -am -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip -DskipITs -Dspotbugs.skip -T 1C -B
else
  echo "No valid connectors to build, skipping connectors build"
fi

mkdir -p /app/pdk/dist

IFS=','; for raw in $CONNECTOR_IDS; do
  id="$(printf '%s' "$raw" | tr -d '[:space:]')"
  [ -z "$id" ] && continue

  if [ -d "/app/tapdata-connectors/connectors/${id}-connector" ]; then
    dir="/app/tapdata-connectors/connectors/${id}-connector/target"
  elif [ -d "/app/tapdata-connectors/connectors-javascript/${id}-connector" ]; then
    dir="/app/tapdata-connectors/connectors-javascript/${id}-connector/target"
  else
    echo "Skip unknown connector id: ${id} (no module dir found)"
    continue
  fi

  jar="$(find "$dir" -maxdepth 1 -type f -name "${id}-connector-v*.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" ! -name "*-original.jar" | head -n1 || true)"
  if [ -z "$jar" ]; then
    jar="$(find "$dir" -maxdepth 1 -type f -name "${id}-connector-*.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" ! -name "*-original.jar" | head -n1 || true)"
  fi

  if [ -z "$jar" ] || [ ! -f "$jar" ]; then
    echo "Skip ${id}: no built jar found under ${dir}"
    continue
  fi

  cp "$jar" "/app/pdk/dist/${id}-connector.jar" || true
done
EOF

# Stage 2: TM Service
FROM eclipse-temurin:17-jdk-jammy AS tm
WORKDIR /app/tm

# Install curl for healthcheck/startup script
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create directory structure
RUN mkdir -p dist/bin dist/lib dist/conf dist/logs dist/connectors/dist

# Copy artifacts from builder (paths adjusted for tapdata/ subdirectory)
COPY --from=builder /app/tapdata/manager/tm/target/tm-*-exec.jar dist/lib/tm.jar
COPY --from=builder /app/tapdata/manager/tm/target/classes/logback.xml dist/conf/
COPY --from=builder /app/tapdata/manager/tm/target/classes/application.yml dist/conf/
COPY --from=builder /app/tapdata/manager/build/start.sh dist/bin/

# Copy PDK deploy jar
COPY --from=builder /app/tapdata/tapdata-cli/target/pdk.jar dist/lib/pdk-deploy.jar

# Set executable permission
RUN chmod +x dist/bin/*.sh

# Set working directory to dist
WORKDIR /app/tm/dist

# Expose TM port
EXPOSE 3030

ARG MONGO_INITDB_ROOT_USERNAME=admin
ARG MONGO_INITDB_ROOT_PASSWORD=tapdata_best2019
ARG MONGO_INITDB_DATABASE=tapdata
ARG MONGO_HOST=mongo
ARG MONGO_PORT=27017

# Environment variables for TM
ENV SERVER_PORT=3000
ENV SPRING_DATA_MONGODB_URI=mongodb://${MONGO_INITDB_ROOT_USERNAME}:${MONGO_INITDB_ROOT_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${MONGO_INITDB_DATABASE}?authSource=admin&replicaSet=rs0
ENV DEFAULT_MONGO_URI=$SPRING_DATA_MONGODB_URI
ENV OBS_MONGO_URI=$SPRING_DATA_MONGODB_URI
ENV LOG_MONGO_URI=$SPRING_DATA_MONGODB_URI
ENV JVM_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED --add-opens=java.base/sun.security.rsa=ALL-UNNAMED --add-opens=java.base/sun.security.x509=ALL-UNNAMED --add-opens=java.base/sun.security.util=ALL-UNNAMED --add-opens=java.xml/com.sun.org.apache.xerces.internal.jaxp.datatype=ALL-UNNAMED -XX:+UnlockExperimentalVMOptions --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-modules=java.se --add-opens=java.management/sun.management=ALL-UNNAMED --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED --add-opens=java.base/jdk.internal.loader=ALL-UNNAMED"

# Entrypoint
CMD ["/bin/sh", "-lc", "java $JVM_OPTS -Xmx4G -Dserver.port=$SERVER_PORT -server -jar lib/tm.jar --spring.config.additional-location=file:conf/ --logging.config=file:conf/logback.xml --spring.data.mongodb.default.uri=${DEFAULT_MONGO_URI} --spring.data.mongodb.obs.uri=${OBS_MONGO_URI} --spring.data.mongodb.log.uri=${LOG_MONGO_URI}"]

# Stage 3: Engine Service
FROM eclipse-temurin:17-jdk-jammy AS engine
WORKDIR /app/engine

# Install curl for startup script (needed for wait loop)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create directory structure
RUN mkdir -p dist/bin dist/lib dist/conf dist/logs /app/pdk

# Copy artifacts from builder
COPY --from=builder /app/tapdata/iengine/ie.jar dist/lib/
COPY --from=builder /app/tapdata/iengine/build/start.sh dist/bin/
COPY --from=builder /app/tapdata/iengine/build/stop.sh dist/bin/
COPY --from=builder /app/tapdata/iengine/build/status.sh dist/bin/
COPY --from=builder /app/tapdata/register_and_start.sh dist/bin/

# Copy PDK and Connectors
COPY --from=builder /app/tapdata/tapdata-cli/target/pdk.jar /app/pdk/
COPY --from=engine-builder /app/pdk/dist/ /app/pdk/dist/

RUN chmod +x dist/bin/*.sh

WORKDIR /app/engine/dist

# Engine needs to know where TM is.
ENV WORK_DIR=/app/engine/dist
ENV JVM_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED --add-opens=java.base/sun.security.rsa=ALL-UNNAMED --add-opens=java.base/sun.security.x509=ALL-UNNAMED --add-opens=java.base/sun.security.util=ALL-UNNAMED --add-opens=java.xml/com.sun.org.apache.xerces.internal.jaxp.datatype=ALL-UNNAMED -XX:+UnlockExperimentalVMOptions --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-modules=java.se --add-opens=java.management/sun.management=ALL-UNNAMED --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED --add-opens=java.base/jdk.internal.loader=ALL-UNNAMED"

# CMD
CMD ["/bin/bash", "-lc", "bin/register_and_start.sh"]
