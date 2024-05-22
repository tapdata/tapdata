#!/bin/bash

# logging functions
daas_log() {
	local type="$1"; shift
	printf '%s [%s] [Entrypoint]: %s\n' "$(date --rfc-3339=seconds)" "$type" "$*"
}
daas_note() {
	daas_log INFO "$@"
}
daas_warn() {
	daas_log Warn "$@" >&2
}
daas_error() {
	daas_log ERROR "$@" >&2
	exit 1
}


# 检查是否是其它脚本引用
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		daas_error "Both $var and $fileVar are set (but are exclusive)"
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}


docker_setup_env() {
	file_env 'MONGODB_USER'
  file_env 'MONGODB_PASSWORD'
  file_env 'MONGODB_CONNECTION_STRING'
  file_env 'BACKENDURL'
  file_env 'MODULE'
}

docker_tapdata_start() {
	daas_note "Waiting for tapdata startup"

  if [ -z "$MODULE" ]; then
    rm -rf ~/.local/*
    chmod +x /tapdata/apps/tapdata
    /tapdata/apps/tapdata start
  else
    rm -rf ~/.local/*
    /tapdata/apps/tapdata start $MODULE
  fi
}


docker_setup_tapdata(){
  if [ -n "$MONGODB_USER" ]; then
    sed -ri "s#username:.*#username: $MONGODB_USER#"  /tapdata/apps/application.yml
  fi

  if [ -n "$MONGODB_PASSWORD" ]; then
    /tapdata/apps/tapdata resetpassword $MONGODB_PASSWORD
  fi

  if [ -z "$MONGODB_CONNECTION_STRING" ]; then
    daas_error "MONGODB_CONNECTION_STRING not set.\n Did you forget to add -e MONGODB_CONNECTION_STRING=... ?"
  else
      sed -ri "s#(mongoConnectionString:.*').*(')#\1$MONGODB_CONNECTION_STRING\2#"  /tapdata/apps/application.yml
  fi

  if [ -n "$BACKENDURL" ]; then
    sed -ri "s#(backendUrl:.*').*(')#\1$BACKENDURL\2#"  /tapdata/apps/application.yml
  fi
}

start_mongodb_if_miss(){
  if [[ "x"$MONGODB_CONNECTION_STRING != "x" ]]; then
    return
  fi

  mkdir -p /tapdata/data/logs
  mongod --dbpath=/tapdata/data/db/ --wiredTigerCacheSizeGB=1 --replSet=rs0 --bind_ip_all --logpath=/tapdata/data/logs/mongod.log --fork
  while [[ 1 ]]; do
    mongo --quiet --eval "db" &> /dev/null
    if [[ $? -eq 0 ]]; then
        break
    fi
    sleep 1
  done
  mongo --quiet --eval 'rs.initiate({"_id":"rs0","members":[{"_id":0,"host":"127.0.0.1:27017"}]})'

  while [[ 1 ]]; do
    mongo --quiet --eval "rs.status()"|grep PRIMARY &> /dev/null
    if [[ $? -eq 0 ]]; then
        break
    fi
    sleep 1
  done
  MONGODB_CONNECTION_STRING="127.0.0.1/tapdata"
}

unzip_files() {
  tar xzf /tapdata/apps/connectors/dist.tar.gz -C /tapdata/apps/connectors/
}

_main() {
  if [[ $WORKDIR != "" && -d /tapdata/apps/components/webroot/ ]]; then
    mkdir -p $WORKDIR/components/webroot/
    cp -r /tapdata/apps/components/webroot/* $WORKDIR/components/webroot/
  fi
  unzip_files
	daas_note "Entrypoint script for tapmanager Server started."
  start_mongodb_if_miss
	# Load various environment variables
	docker_setup_env "$@"
  docker_setup_tapdata
	daas_note "Starting tapdata server"
	docker_tapdata_start
	daas_note "tapdata server started."
    rm -rf /tapdata/apps/connectors/dist/*
	exec "$@"
}

# 如果是其它脚本引用，则不执行操作
if ! _is_sourced; then
	_main "$@"
fi

while [[ 1 ]]; do
  sleep 10
done