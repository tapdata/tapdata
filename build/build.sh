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

while getopts 'c:l:u:p:t:o:' OPT; do
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
EOF

IFS=" " read -r -a COMPONENTS <<<"$(echo "$COMPONENT_NAME" | tr -d ' ' | tr ',' ' ')"

build_java_component() {
  if [[ "$RUN_UNITTEST" == "false" ]]; then
    run_unittest="-DskipTests"
  fi
  mvn install $run_unittest $BUILD_ARGS
}

# build component
for COMPONENT in ${COMPONENTS[@]}; do
  if [[ $COMPONENT == "tapdata" ]]; then
    cd $TAPDATA_DIR && build_java_component
  elif [[ $COMPONENT == "connectors" ]]; then
    cd $CONNECTOR_DIR && build_java_component
  elif [[ $COMPONENT == "frontend" ]]; then
    cd $FRONTEND_DIR && DAAS_BUILD_NUMBER=$TAG_NAME bash build/build.sh -m $FRONTEND_BUILD_MODE
  fi
done

make_package_tapdata() {
  mkdir -p $OUTPUT_DIR/etc/init/ $OUTPUT_DIR/components/ $OUTPUT_DIR/lib/
  mkdir -p $OUTPUT_DIR/bin/manager/ $OUTPUT_DIR/bin/iengine/
  cd $OUTPUT_DIR/
  cp $TAPDATA_DIR/manager/tm/target/classes/logback.xml etc/logback.xml
  cp $TAPDATA_DIR/manager/tm/target/classes/application.yml etc/application-tm.yml
  mkdir -p components/
  cp $TAPDATA_DIR/manager/tm/target/tm-*-exec.jar components/tm.jar
  cp $TAPDATA_DIR/iengine/ie.jar components/tapdata-agent.jar
  cp $TAPDATA_DIR/tapdata-cli/target/pdk.jar lib/pdk-deploy.jar
  # copy script files to start manager and iengine
  cp $TAPDATA_DIR/manager/build/start.sh $OUTPUT_DIR/bin/manager/start.sh
  cp $TAPDATA_DIR/iengine/build/start.sh $OUTPUT_DIR/bin/iengine/start.sh
}

make_package_connectors() {
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
  cd $OUTPUT_DIR/
  cp $TAPDATA_DIR/build/image/Dockerfile .
  cp $TAPDATA_DIR/build/image/docker-entrypoint.sh .
  docker build -t ghcr.io/tapdata/tapdata:$TAG_NAME .
}

make_tar() {
  cd $OUTPUT_DIR/
  cp $TAPDATA_DIR/build/image/docker-entrypoint.sh ./start.sh
  chmod +x start.sh
  tar cfz tapdata-$TAG_NAME.tar.gz *
}

# make output
if [[ $OUTPUT_TYPE == "docker" ]]; then
  make_docker
elif [[ $OUTPUT_TYPE == "tar" ]]; then
  make_tar
fi
