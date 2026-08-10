#!/bin/bash

# load log.sh
# script base dir
SCRIPT_BASE_DIR=$(dirname "$0")
. "$SCRIPT_BASE_DIR/log.sh"

# run unit test
RUN_UNITTEST="false"
# component name to be build or all (tapdata, connectors, frontend)
COMPONENT_NAME=""
# build args
BUILD_ARGS=""
# projects directory
PROJECT_ROOT_DIR=$(cd "$SCRIPT_BASE_DIR/.." && pwd)
# tapdata directory
TAPDATA_DIR="$PROJECT_ROOT_DIR/../tapdata"
# connector directory
CONNECTOR_DIR="$PROJECT_ROOT_DIR/../tapdata-connectors"
# frontend directory
FRONTEND_DIR="$PROJECT_ROOT_DIR/../tapdata-enterprise-web"
# frontend build mode
FRONTEND_BUILD_MODE="community"
# tag name
TAG_NAME="latest"
# OUTPUT_DIR
OUTPUT_DIR="$PROJECT_ROOT_DIR/output"
# package components
PACKAGE_COMPONENTS=""
# output type (docker or tar)
OUTPUT_TYPE=""
# filter connectors list
CONNECTORS_LIST=$(cat $SCRIPT_BASE_DIR/.connectors_list)
# platform
PLATFORM=$(uname -m)

while getopts 'c:l:u:p:t:o:m:' OPT; do
	case "$OPT" in
	c)
    COMPONENT_NAME="$OPTARG"
    ;;
  p)
    PACKAGE_COMPONENTS="$OPTARG"
    ;;
  u)
    RUN_UNITTEST="$OPTARG"
    ;;
  l)
    BUILD_ARGS="$OPTARG"
    ;;
  t)
    TAG_NAME="$OPTARG"
    ;;
  o)
    OUTPUT_TYPE="$OPTARG"
    ;;
  m)
    PLATFORM="$OPTARG"
    ;;
  esac
done

info "The Env Setting List:"
cat <<EOF
  COMPONENT_NAME:     $COMPONENT_NAME
  PACKAGE_COMPONENTS: $PACKAGE_COMPONENTS
  RUN_UNITTEST:       $RUN_UNITTEST
  BUILD_ARGS:         $BUILD_ARGS
  TAG_NAME:           $TAG_NAME
  OUTPUT_TYPE:        $OUTPUT_TYPE
  PLATFORM:           $PLATFORM
EOF

IFS=" " read -r -a COMPONENTS <<<"$(echo "$COMPONENT_NAME" | tr -d ' ' | tr ',' ' ')"

build_java_component() {
  if [[ "$RUN_UNITTEST" == "false" ]]; then
    run_unittest="-DskipTests"
  fi
  mvn install -T1C $run_unittest $BUILD_ARGS
}

function switch_node_version() {
  version=$1
  export NVM_DIR="$HOME/.nvm"
  [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
  if ! command -v nvm > /dev/null 2>&1; then
    curl -o- https://gitee.com/RubyMetric/nvm-cn/raw/main/install.sh | bash
    chmod +x ~/.nvm/nvm.sh
    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" # This loads nvm
    export NVM_NODEJS_ORG_MIRROR=https://npmmirror.com/mirrors/node
  fi
  nvm use $version
  if [[ $? -ne 0 ]]; then
    nvm install $version
    nvm use $version
  fi
}

# build component
for COMPONENT in ${COMPONENTS[@]}; do
  if [[ $COMPONENT == "tapdata" ]]; then
    cd $TAPDATA_DIR && build_java_component
  elif [[ $COMPONENT == "connectors" ]]; then
    cd $CONNECTOR_DIR && build_java_component
  elif [[ $COMPONENT == "frontend" ]]; then
    switch_node_version 20
    cd $FRONTEND_DIR && DAAS_BUILD_NUMBER=$TAG_NAME bash build/build.sh -m $FRONTEND_BUILD_MODE
  fi
done

make_package_tapdata() {
  mkdir -p $OUTPUT_DIR/etc/init/ $OUTPUT_DIR/components/ $OUTPUT_DIR/lib/
  cd $OUTPUT_DIR/
  cp $TAPDATA_DIR/manager/tm/target/classes/logback.xml etc/logback.xml
  cp $TAPDATA_DIR/manager/tm/target/classes/application.yml etc/application-tm.yml
  mkdir -p components/
  cp $TAPDATA_DIR/manager/tm/target/tm-*-exec.jar components/tm.jar
  cp $TAPDATA_DIR/iengine/ie.jar components/tapdata-agent.jar
  cp $TAPDATA_DIR/tapdata-cli/target/pdk.jar lib/pdk-deploy.jar

  # Copy openapi-generator directory to etc
  if [[ -d "$TAPDATA_DIR/openapi-generator" ]]; then
    info "Copying openapi-generator directory to etc/"
    cp -r $TAPDATA_DIR/openapi-generator etc/
  else
    warn "openapi-generator directory not found at $TAPDATA_DIR/openapi-generator"
  fi
}

make_package_connectors() {
  # filter connectors
  mv $CONNECTOR_DIR/connectors/dist $CONNECTOR_DIR/connectors/backup
  mkdir -p $CONNECTOR_DIR/connectors/dist/
  for item in $CONNECTORS_LIST; do
    find $CONNECTOR_DIR/connectors/backup/ -type f -name "${item}" | xargs -I {} mv {} $CONNECTOR_DIR/connectors/dist/
  done

  mkdir -p $OUTPUT_DIR/connectors/dist/
  cd $OUTPUT_DIR/
  tar cfz connectors/dist.tar.gz -C $CONNECTOR_DIR/connectors/ dist/
}

make_package_frontend() {
  mkdir -p $OUTPUT_DIR/components/webroot/
  cd $OUTPUT_DIR/
  cp -r $FRONTEND_DIR/dist/* components/webroot/
}

# make package
if [[ $PACKAGE_COMPONENTS == "tapdata" ]]; then
  make_package_tapdata
elif [[ $PACKAGE_COMPONENTS == "connectors" ]]; then
  make_package_connectors
elif [[ $PACKAGE_COMPONENTS == "frontend" ]]; then
  make_package_frontend
elif [[ $PACKAGE_COMPONENTS == "all" ]]; then
  make_package_tapdata
  make_package_connectors
  make_package_frontend
fi

make_docker() {
  local builder_name
  local cache_scope
  local cache_ref
  local profiler_arch
  local target_arch
  local build_status

  cd "$OUTPUT_DIR/"
  cp "$TAPDATA_DIR/build/image/Dockerfile" .
  cp "$TAPDATA_DIR/build/image/docker-entrypoint.sh" .
  cp -r "$TAPDATA_DIR/build/image/bin" .
  cp -r "$TAPDATA_DIR/build/image/supervisor" .
  printf '{"app_version":"%s"}\n' "$TAG_NAME" > ./.version

  info ">> download and prepare async-profiler for amd64 and arm64..."
  rm -rf ./docker-assets/async-profiler
  mkdir -p ./docker-assets/async-profiler
  for profiler_arch in x64 arm64; do
    if [[ $profiler_arch == "x64" ]]; then
      target_arch="amd64"
    else
      target_arch="arm64"
    fi
    rsync -vzrt --password-file=/tmp/rsync.passwd \
      "rsync://root@192.168.1.184:873/data/enterprise-artifact/tools/async-profiler-3.0-linux-$profiler_arch.tar.gz" \
      ./async-profiler.tar.gz
    tar -xzf ./async-profiler.tar.gz -C ./docker-assets/async-profiler
    mv "./docker-assets/async-profiler/async-profiler-3.0-linux-$profiler_arch" \
      "./docker-assets/async-profiler/$target_arch"
  done
  rm -f ./async-profiler.tar.gz

  builder_name="tapdata-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-$$"
  cache_scope=$(printf '%s' "${TAPDATA_CACHE_SCOPE:-${GITHUB_REF_NAME:-default}}" | tr '/:@' '-' | tr -cd '[:alnum:]_.-')
  cache_scope="cache-${cache_scope:0:100}"
  cache_ref="${TAPDATA_CACHE_REPOSITORY:-harbor.internal.tapdata.io/tapdata/tapdata-build-cache}:${cache_scope}"

  trap "docker buildx rm '$builder_name' >/dev/null 2>&1 || true; rm -rf './docker-assets'" EXIT

  info ">> Install QEMU..."
  docker run --privileged --rm tonistiigi/binfmt --install all
  info ">> create isolated builder: $builder_name"
  docker buildx create --name "$builder_name" --driver docker-container --use
  docker buildx inspect "$builder_name" --bootstrap

  info ">> registry cache: $cache_ref"
  info ">> building..."
  docker buildx build \
    --builder "$builder_name" \
    --platform linux/amd64,linux/arm64 \
    --build-arg "BASE_IMAGE=${TAPDATA_BASE_IMAGE:-ghcr.io/tapdata/base:0.2}" \
    --cache-from "type=registry,ref=$cache_ref" \
    --cache-to "type=registry,ref=$cache_ref,mode=max,ignore-error=true" \
    --tag "${TAPDATA_IMAGE_REPOSITORY:-harbor.internal.tapdata.io/tapdata/tapdata}:$TAG_NAME" \
    --push \
    .
  build_status=$?

  docker buildx rm "$builder_name" || true
  rm -rf ./docker-assets
  trap - EXIT
  return "$build_status"
}

make_tar() {
  cd $OUTPUT_DIR/
  cp $TAPDATA_DIR/build/image/docker-entrypoint.sh ./start.sh
  rsync -a $TAPDATA_DIR/build/image/bin/ ./
  rsync -a $TAPDATA_DIR/build/image/supervisor ./
  # download async-profile
  if [[ $PLATFORM == "x86_64" ]]; then
    rsync -vzrt --password-file=/tmp/rsync.passwd rsync://root@192.168.1.184:873/data/enterprise-artifact/tools/async-profiler-3.0-linux-x64.tar.gz ./async-profiler.tar.gz
  else
    rsync -vzrt --password-file=/tmp/rsync.passwd rsync://root@192.168.1.184:873/data/enterprise-artifact/tools/async-profiler-3.0-linux-arm64.tar.gz ./async-profiler.tar.gz
  fi
  tar -xzf async-profiler.tar.gz -C ./components/
  mv ./components/async-profiler-* ./components/async-profiler
  chmod +x start.sh stop.sh status.sh
  tar cfz tapdata-$PLATFORM-$TAG_NAME.tar.gz *
}

# make output
if [[ $OUTPUT_TYPE == "docker" ]]; then
  make_docker
elif [[ $OUTPUT_TYPE == "tar" ]]; then
  make_tar
fi
