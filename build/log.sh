#!/bin/bash
header="[tapdata]"
error() {
    echo -e "["`date +'%D %T'`"]" ${header}:"\033[31m $1 \033[0m"
    exit -1
}

info() {
    echo -e "["`date +'%D %T'`"]" ${header}:"\033[36m $1 \033[0m"
}

notice() {
    echo -e "["`date +'%D %T'`"]" ${header}:"\033[32m $1 \033[0m"
}

warn() {
    echo -e "["`date +'%D %T'`"]" ${header}:"\033[33m $1 \033[0m"
}
