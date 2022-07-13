#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath

error() {
    echo -e "["`date +'%D %T'`"]" ${header}:"\033[31m $1 \033[0m"
    exit 1
}

info() {
    echo -e "["`date +'%D %T'`"]" ${header}:"\033[36m $1 \033[0m"
}

which pytest &> /dev/null
if [[ $? -ne 0 ]]; then
    error "no pytest module found, please run pip install pytest first"
fi

cd $basepath/test/

pytest --alluredir=./allure-results

retCode=$?

if [[ -d "../report-test/last-history" ]]; then
    cp -r ../report-test/last-history/* ./allure-results/history
fi
allure generate ./allure-results -o ./report/integrate.html

mkdir -p gh_pages
cp -r ./report/integrate.html/* gh_pages/${{ github.run_id }}-${{ github.run_number }}-${{ github.run_attempt }}
cp -r ./report/integrate.html/history/* gh_pages/last-history

ls -al gh_pages

exit $retCode
