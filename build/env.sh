# If you want build tapdata in docker, please set this is to "docker"
# Or set to to "local", please install jdk and maven before building from local
export tapdata_build_env="docker"
export tapdata_run_env="docker"

# TODO: component memory config, percent or 512MB/1GB both ok
export database_mem="0.3"
export manager_mem="0.2"
export iengine_mem="0.2"

export build_container_name="tapdata-build-container"
export dev_container_name="tapdata-all-in-one-dev"
export use_container_name="tapdata-all-in-one-dev"
export tapdata_build_image="ghcr.io/tapdata/tapdata/tldp/build:0.3"
export LC_ALL=en_US.UTF-8
export _in_docker=""
if [[ -f "/.dockerenv" ]]; then
    export _in_docker="yes"
fi
