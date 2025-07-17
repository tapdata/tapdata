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

# build component
for COMPONENT in ${COMPONENTS[@]}; do
  if [[ $COMPONENT == "tapdata" ]]; then
    cd $TAPDATA_DIR && build_java_component
  elif [[ $COMPONENT == "connectors" ]]; then
    cd $CONNECTOR_DIR && build_java_component
  elif [[ $COMPONENT == "frontend" ]]; then
    nvm install 20
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
  cd $OUTPUT_DIR/
  cp $TAPDATA_DIR/build/image/Dockerfile .
  cp $TAPDATA_DIR/build/image/docker-entrypoint.sh .
  cp -r $TAPDATA_DIR/build/image/bin .
  cp -r $TAPDATA_DIR/build/image/supervisor .
  
  # download and prepare async-profiler 
  mkdir -p ./components/
  if [[ $PLATFORM == "x86_64" ]]; then
    rsync -vzrt --password-file=/tmp/rsync.passwd rsync://root@192.168.1.184:873/data/enterprise-artifact/tools/async-profiler-3.0-linux-x64.tar.gz ./async-profiler.tar.gz
  else
    rsync -vzrt --password-file=/tmp/rsync.passwd rsync://root@192.168.1.184:873/data/enterprise-artifact/tools/async-profiler-3.0-linux-arm64.tar.gz ./async-profiler.tar.gz
  fi
  tar -xzf async-profiler.tar.gz -C ./components/
  mv ./components/async-profiler-* ./components/async-profiler
  rm -f ./async-profiler.tar.gz
  
  # docker build -t harbor.internal.tapdata.io/tapdata/tapdata:$TAG_NAME .
  docker buildx create --use --name multi-platform --platform linux/amd64,linux/arm64
  docker buildx build --platform linux/arm64,linux/amd64 -t harbor.internal.tapdata.io/tapdata/tapdata:$TAG_NAME . --push
}

function make_and_push_docker_image() {
  info "Start Make and Push Docker Image."

  echo "{\"oem\":\"$FRONTEND_BUILD_MODE\"}" > $OUTPUT_DIR/.config
  echo "{\"app_version\":\"$TAG_NAME\"}" > $OUTPUT_DIR/.version

  tar cfz $PROJECT_ROOT_DIR/output/connectors/dist.tar.gz -C $PROJECT_ROOT_DIR/output/connectors/ dist/
  cp $PROJECT_ROOT_DIR/build/image/Dockerfile $PROJECT_ROOT_DIR/output/
  cp $PROJECT_ROOT_DIR/build/image/docker-entrypoint.sh $PROJECT_ROOT_DIR/output/

  if [[ $REPLACE_FRONTEND_MODE == "true" ]]; then
    rsync -vzrt --password-file=/tmp/rsync.passwd rsync://root@58.251.34.123:873/data/enterprise-artifact/tools/service_control $PROJECT_ROOT_DIR/output/
    chmod +x $PROJECT_ROOT_DIR/output/service_control
    sed -i 's/COPY tapdata \/tapdata\/apps\/tapdata/COPY service_control \/tapdata\/apps\/service_control/' $PROJECT_ROOT_DIR/output/Dockerfile

    mv $PROJECT_ROOT_DIR/output/components/tapdata-agent.jar $PROJECT_ROOT_DIR/output/components/app-agent.jar
    rm -rf $PROJECT_ROOT_DIR/output/components/webroot/docs
  fi

  if [[ "$EXCLUDE_APISERVER" == "true" ]]; then
    rm -rf $PROJECT_ROOT_DIR/output/components/apiserver.tar.gz
    info "Removed apiserver.tar.gz from docker image (excluded)."
  fi

  echo 'Gotapd8!' | docker login $DOCKER_REGISTRY --username=cicd --password-stdin
  docker build $PROJECT_ROOT_DIR/output/ -t $DOCKER_REPOSITORY/tapdata-enterprise:$TAG_NAME
  if [[ $? -eq 0 ]]; then
    info "Build Docker Image Success."
  else
    error "Build Docker Image Failed."
  fi
  # gcloud auth login --cred-file="$PROJECT_ROOT_DIR/build/keyfile.json" -q
  # gcloud auth configure-docker $DOCKER_REGISTRY -q
  # docker login -u flow-engine-1702457090174 -p 01b4d845fa04a6ee2ca72e669bac7b121e29ccf6 tapdata-docker.pkg.coding.net
  echo 'Gotapd8!' | docker login $DOCKER_REGISTRY --username=cicd --password-stdin
  docker push $DOCKER_REPOSITORY/tapdata-enterprise:$TAG_NAME
  if [[ $? -eq 0 ]]; then
    info "Push docker image successfully, tag: $TAG_NAME"
  else
    error "Push docker image failed, tag: $TAG_NAME"
  fi
  rm -rf $PROJECT_ROOT_DIR/output/connectors/dist.tar.gz
  rm -rf $PROJECT_ROOT_DIR/output/Dockerfile
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
  make_and_push_docker_image
elif [[ $OUTPUT_TYPE == "tar" ]]; then
  make_tar
fi
