#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
sourcepath=$(cd `dirname $0`/../; pwd)
. $basepath/log.sh
. $basepath/env.sh

ulimit -c unlimited

if [[ $tapdata_build_env == "docker" && $_in_docker == "" ]]; then
    which docker &> /dev/null
    if [[ $? -ne 0 ]]; then
        error "no docker found, please install it before build package"
    fi
    docker images $tapdata_build_image &> /dev/null
    if [[ $? -ne 0 ]]; then
        docker pull $tapdata_build_image
    fi
fi

cd $basepath
if [[ $_in_docker == "" ]]; then
    notice "tapdata live data platform start building..."
fi

image=`cat $basepath/image/tag`
is_build="false"
is_package="false"
output=""

usage() {
    echo "Usage: $0 [-c] [-p] [-o jar|tar|docker] [-d] [-t image]"
    echo "  -c: build project, default is false"
    echo "  -p: package project, default is false"
    echo "  -o: output type, default is blank, optional value: image"
    echo "  -d: clean project outputs, default is false"
    echo "  -t: docker image tag"
    echo "  -h: help"
    exit 1
}

while getopts 'c:p:o:d::t:' OPT; do
	case "$OPT" in
	'c')
    is_build=$(echo "$OPTARG" | tr "[A-Z]" "[a-z]")
		;;
	'p')
		is_package=$(echo "$OPTARG" | tr "[A-Z]" "[a-z]")
		;;
	'd')
    clean
    exit 0
		;;
	'o')
    output="$OPTARG"
		;;
  ?)
    usage
    exit 1
    ;;
	esac
done

cat <<_END_
tapdata build env:    $tapdata_build_env
tapdata build image:  $tapdata_build_image
tapdata build output: $output
is_build:             $is_build
is_package:           $is_package
_in_docker:           $_in_docker

_END_

check_env() {
    which java &> /dev/null
    if [[ $? -ne 0 ]]; then
        error "no java found, please install it before build package"
    fi

    which mvn &> /dev/null
    if [[ $? -ne 0 ]]; then
        error "no mvn found, please install it before build package"
    fi
}

build() {
  # 1. Build project in docker.
  # 2. run check_env function to check if java and mvn installed.
  # 3. Run build command in local.
  #
  # Provide two ways to build project, in docker or in local.
  # If you build in docker, it will pull a docker image that cached all dependencies and then build project in it.
  # If you build in local, it will use your local environment to build project.
  # Run `mvn clean install -DskipTests` to build project in local and in docker.
  # You can set env variable in env.sh file. Explanation of Environment Variables Used here:
  #   $tapdata_build_env: docker or local
  #   $_in_docker: if in docker, this variable will be set to yes
  #   $tapdata_build_image: the docker build image which cached all dependencies
  # 1. Build project in docker.
  if [[ $tapdata_build_env == "docker" && $_in_docker == "" ]]; then
    # if tapdata-build-container not running
    docker ps | grep tapdata-build-container &> /dev/null
    if [[ $? -ne 0 ]]; then
      # if tapdata-build-container stopped or not exist
      docker ps -a | grep tapdata-build-container &> /dev/null
      if [[ $? -eq 0 ]]; then
        info "tapdata build container stopped, try start it..."
        docker start tapdata-build-container
      else
        info "no tapdata build container find, try run a new one..."
        docker run --name=tapdata-build-container -v $sourcepath:/tapdata-source/ -i $tapdata_build_image bash -c "while true; do sleep 1; done"
      fi
    fi
    docker exec -e PRODUCT=idaas -i tapdata-build-container bash -c "cd /tapdata-source && bash build/build.sh -c true"
  fi
  # 2. run check_env function to check if java and mvn installed.
  check_env
  # 3. Run build command in local.
  cd $sourcepath && mvn clean install -DskipTests
}

make_iengine_dist() {
  # Collect Iengine outputs.
  #
  # Collect all the outputs from Iengine and copy them to the "dist" directory.
  IENGINE_PATH=$sourcepath/iengine  # iengine path
  IENGINE_SBIN_FILE="ie.jar"  # iengine sbin file
  # make iengine dist
  cd $IENGINE_PATH && mkdir -p dist dist/bin dist/lib dist/conf dist/logs
  cd $IENGINE_PATH && cp $IENGINE_SBIN_FILE dist/lib
  # copy script to dist
  cd $IENGINE_PATH && cp build/start.sh dist/bin
  cd $IENGINE_PATH && cp build/stop.sh dist/bin
  cd $IENGINE_PATH && cp build/status.sh dist/bin
}

make_manager_dist() {
  # Collect Manager outputs.
  #
  # Collect all the outputs from Manager and copy them to the "dist" directory.
  MANAGER_PATH=$sourcepath/manager  # manager path
  MANAGER_SBIN_FILE="tm-*.jar"  # manager sbin file
  # make manager dist
  cd $MANAGER_PATH && mkdir -p dist dist/bin dist/lib dist/conf dist/logs
  cd $MANAGER_PATH && cp "tm/target/classes/logback.xml" dist/conf
  cd $MANAGER_PATH && cp "tm/target/classes/application.yml" dist/conf
  cd $MANAGER_PATH && f=`find tm/target/ -name $MANAGER_SBIN_FILE` && cp $f dist/lib
  # copy script to dist
  cd $MANAGER_PATH && cp build/start.sh dist/bin
}

make_connector_dist() {
  # Collect Connector outputs and make a tarball.
  cd $sourcepath/connectors && tar czf dist.tar.gz dist && cd -
}

make_dist() {
  # 1. Collect Iengine outputs.
  # 2. Collect Manager outputs.
  # 3. Collect Connector outputs.
  #
  # Collect all the outputs from various components and copy them to the "dist" directory.

  # 1. Collect Iengine outputs.
  make_iengine_dist

  # 2. Collect Manager outputs.
  make_manager_dist

  # 3. Collect Connector outputs.
  make_connector_dist
}

package_outputs() {
  # 1. Make dist directory.
  # 2. Package Iengine outputs.
  # 3. Package Manager outputs.
  # 4. Package Connector outputs.
  # 5. package pdk.jar outputs.
  # 6. Print cost time.
  #
  # Package all the outputs from various components and copy them to the "dist" directory to make a tarball or docker image.

  # 1. Make dist directory.
  START_TIME=`date '+%s'`
  make_dist
  mkdir -p $sourcepath/dist $sourcepath/dist/iengine $sourcepath/dist/manager $sourcepath/dist/connectors
  # 2. Package Iengine outputs.
  cp -r $sourcepath/iengine/dist/* $sourcepath/dist/iengine/
  # 3. Package Manager outputs.
  cp -r $sourcepath/manager/dist/* $sourcepath/dist/manager/
  # 4. Package Connector outputs.
  cp -r $sourcepath/connectors/dist.tar.gz $sourcepath/dist/connectors/
  # 5. package pdk.jar outputs.
  cp -r $sourcepath/tapdata-cli/target/pdk.jar $sourcepath/dist/
  # 6. Print cost time.
  END_TIME=`date '+%s'`
  DURATION=`expr $END_TIME - $START_TIME`
  info "package outputs success, cost time: $DURATION seconds"
}

make_image() {
  # 1. Check if docker installed.
  # 2. Cp image directory to dist directory.
  # 3. Make docker image.
  #
  # Make a docker image from the "dist" directory.

  # 1. Check if docker installed.
  which docker &> /dev/null
  if [[ $? -ne 0 ]]; then
    error "docker not installed, please install docker first."
    exit 1
  fi
  # 2. Cp image directory to dist directory.
  mkdir -p $sourcepath/dist/image
  cp -r $sourcepath/build/image/* $sourcepath/dist/
  # 3. Make docker image.
  cd $sourcepath/dist && bash build.sh
}

clean() {
  # Clean all the outputs from various components.
  rm -rf $sourcepath/dist
  rm -rf $sourcepath/iengine/dist
  rm -rf $sourcepath/manager/dist
  rm -rf $sourcepath/connectors/dist
  rm -rf $sourcepath/connectors/dist.tar.gz
}

if [[ $is_build == "true" ]]; then
    build
fi

if [[ $is_package == "true" && $_in_docker == "" ]]; then
    package_outputs
fi

if [[ $output == "image" && $_in_docker == "" ]]; then
    make_image
fi
