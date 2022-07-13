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

if [[ -d "../../report-test/last-history" ]]; then
    cp -r ../../report-test/last-history/* ./allure-results/history
fi

allure generate ./allure-results -o ./report/integrate.html

echo $ACTION_SIGN
if [[ "x"$ACTION_SIGN == "x" ]]; then
    ACTION_SIGN=1
fi

mkdir -p gh_pages/$ACTION_SIGN
mkdir -p gh_pages/last-history

cp -r ./report/integrate.html/* gh_pages/$ACTION_SIGN
cp -r ./report/integrate.html/history/* gh_pages/last-history

cp ../index.html.template index.html

sed -i "s:__number__:$ACTION_SIGN:g" index.html
cp index.html gh_pages/

mv gh_pages/ ../../

exit $retCode
