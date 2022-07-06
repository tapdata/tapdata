error() {
    echo -e "["`date +'%D %T'`"]" ${header}:"\033[31m $1 \033[0m"
    exit 1
}

which pytest &> /dev/null
if [[ $? -ne 0 ]]; then
    error "no pytest module found, please run pip install pytest first"
fi

test -f ./.env
if [[ $? -ne 0 ]]; then
    echo $TEST_DATABASE > ./.env
fi

pytest
