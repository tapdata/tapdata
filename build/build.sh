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

components=""
all_components=("plugin-kit" "file-storages" "connectors-common" "manager" "iengine" "connectors" "tapdata-cli")
package_all_components=("iengine" "manager")
source_components=("plugin-kit" "file-storages" "connectors-common" "connectors" "tapshell" "build")
output="package"

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

build_component() {
    start_time=`date '+%s'`
    _component=$1
    info "$_component start building..."
    p=`pwd`
    cd $basepath
    cd ..
    if [[ ! -d  $_component ]]; then
        warn "no path $_component found, skip build module $_component"
        return 0
    fi
    cd $_component && bash build/build.sh
    if [[ $? -ne 0 ]]; then
        cd $p
        end_time=`date '+%s'`
        duration=`expr $end_time - $start_time`
        if [[ $_component == "connectors" ]]; then
            # connectors build fail will continue build
            warn "connectors build fail, cost time: $duration seconds, continue building..."
            return
        else
            error "$_component build fail, cost time: $duration seconds, stop building..."
        fi
        exit 1
    fi
    cd $p
    end_time=`date '+%s'`
    duration=`expr $end_time - $start_time`
    info "$_component build success, cost time: $duration seconds"
}

register_connectors() {
    info "please start manager, then run `` to register connectors"
}

build() {
    if [[ $tapdata_build_env == "docker" && $_in_docker == "" ]]; then
        docker ps | grep tapdata-build-container &> /dev/null
        if [[ $? -ne 0 ]]; then
            docker ps -a | grep tapdata-build-container &> /dev/null
            if [[ $? -eq 0 ]]; then
                info "tapdata build container stopped, try start it..."
                docker start tapdata-build-container
            else
                info "no tapdata build container find, try run a new one..."
                docker run --name=tapdata-build-container -v $sourcepath:/tapdata-source/ -id $tapdata_build_image bash
            fi
        fi
        # when add env PRODUCT=idaas, tm will init database
        docker exec -e PRODUCT=idaas -i tapdata-build-container bash -c "cd /tapdata-source && bash build/build.sh -c $components"
        if [[ $? -ne 0 ]]; then
            exit 1
        fi
        return
    fi
#    echo "env PRODUCT is: ${PRODUCT}"
    check_env
    for component in ${all_components[*]}
    do
        if [[ $components == $component || $components == "all" ]]; then
            build_component $component
        fi
    done
}

package() {
    start_time=`date '+%s'`
    tmp_build=$basepath/../tmp_dist
    final_build=$basepath/../dist
    rm -rf $tmp_build
    mkdir -p $tmp_build
    for component in ${package_all_components[*]}
    do
        mkdir -p $tmp_build/$component
        if [[ ! -d $basepath/../$component/dist ]]; then
            error "build path $component/dist not found, stop package"
        fi
        cp -r $basepath/../$component/dist/* $tmp_build/$component
    done



    rm -rf $basepath/../tapshell/.register
    for source_component in ${source_components[*]}
    do
        mkdir -p $tmp_build/$source_component
        cp -r $basepath/../$source_component/* $tmp_build/$source_component
    done

    mkdir -p $tmp_build/bin/

    rm -rf $final_build
    mv $tmp_build $final_build
    end_time=`date '+%s'`
    duration=`expr $end_time - $start_time`
    info "package success, package is: $final_build, cost time: $duration seconds"
}

clean() {
    cd $sourcepath
    start_time=`date '+%s'`
    info "start clean build package..."
    rm -rf dist
    rm -rf tmp_dist
    for component in ${all_components[*]}
    do
        rm -rf $component/dist
        for c in `ls $component`; do
            rm -rf $component/$c/target
        done
    done
    rm -rf temp

    end_time=`date '+%s'`
    duration=`expr $end_time - $start_time`
    info "all component build package clean success, cost time:  $duration seconds"
}

make_image() {
    which docker &> /dev/null
    if [[ $? -ne 0 ]]; then
        error "no docker find in system, please install it before making a image"
    fi

    mkdir -p $basepath/../dist/image
    cp -r $basepath/../build/image/* $basepath/../dist/image
    ls $basepath/../dist/ | grep -v image | xargs -I {} cp -r $basepath/../dist/{} $basepath/../dist/image/
    cd $basepath/../dist/image && bash build.sh
    cd $basepath/../
}

is_build=0
is_package=0
output="jar"

while getopts 'c:p:o:d:' opt; do
	case "$opt" in
	'c')
		components="$OPTARG"
        is_build=1
		;;
	'p')
		is_package=1
		;;
	'd')
        clean
        exit 0
		;;
	'o')
        output="$OPTARG"
        is_package=1
		;;
	esac
done

if [[ $is_build -eq 1 ]]; then
    build
fi

if [[ $is_package -eq 1 && $_in_docker == "" ]]; then
    package
fi

if [[ $output == "image" && $_in_docker == "" ]]; then
    make_image
fi
