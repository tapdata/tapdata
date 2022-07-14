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
mkdir -p ./allure-results/history

retCode=$?

mv ../../build/filter_history.py ../
mv ../../build/template ../

if [[ -d "../../report-test/last-history" ]]; then
    cp -r ../../report-test/last-history/* ./allure-results/history
    python ../filter_history.py ./allure-results/history
fi

allure generate ./allure-results -o ./report/integrate.html

if [[ "x"$RUN_SIGN == "x" ]]; then
    RUN_SIGN="1-1-1"
fi

BRANCH_DIR=`echo $BRANCH | sed "s:/:-:g"`

mkdir -p gh_pages/$BRANCH_DIR/$RUN_SIGN
mkdir -p gh_pages/last-history

cp -r ./report/integrate.html/* gh_pages/$BRANCH_DIR/$RUN_SIGN
cp -r ./report/integrate.html/history/* gh_pages/last-history

cp -r ../template/* ./

sed -i "s:__run_sign__:$RUN_SIGN:g" index.html
sed -i "s:__branch_dir__:$BRANCH_DIR:g" index.html
sed -i "s:__run_sign__:$RUN_SIGN:g" executors.json
sed -i "s:__run_number__:$RUN_NUMBER:g" executors.json
sed -i "s:__run_id__:$RUN_ID:g" executors.json
sed -i "s:__branch_dir__:$BRANCH_DIR:g" executors.json

cp index.html gh_pages/
cp executors.json gh_pages/$RUN_SIGN/widgets/
cp environment.json gh_pages/$RUN_SIGN/widgets/

mv gh_pages/ ../../

exit $retCode
