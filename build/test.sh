#!/bin/bash
basepath="/tapdata-source/tapshell"
sourcepath="/tapdata-source"
pkgpath="/tapdata"

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

cd $basepath/test

info "start pytest..."

pytest --alluredir=./allure-results

info "pytest done."

mkdir -p ./allure-results/history

cp $sourcepath/build/filter_history.py ../
cp -r $sourcepath/build/template ../

BRANCH_DIR=`echo $BRANCH | sed "s:/:-:g"`

if [[ -d "$sourcepath/report-test/$BRANCH_DIR/last-history" ]]; then

    info "start filter_history.py"

    cp -r $sourcepath/report-test/$BRANCH_DIR/last-history/* ./allure-results/history
    python ../filter_history.py ./allure-results/history

    info "filter_history done."
fi

info "start allure generate"

allure generate ./allure-results -o ./report/integrate.html

info "allure generate done."

if [[ "x"$RUN_SIGN == "x" ]]; then
    RUN_SIGN="1-1-1"
fi

mkdir -p gh_pages/$BRANCH_DIR/$RUN_SIGN
mkdir -p gh_pages/$BRANCH_DIR/last-history

cp -r ./report/integrate.html/* gh_pages/$BRANCH_DIR/$RUN_SIGN
cp -r ./report/integrate.html/history/* gh_pages/$BRANCH_DIR/last-history

cp -r ../template/* ./

info "updating index.html"

if [[ $BRANCH_DIR == "refs-heads-master" ]]; then
  DEFAULT_DIR="refs-heads-master"
  sed -i "s:__run_sign__:$RUN_SIGN:g" index.html
  sed -i "s:__branch_dir__:$DEFAULT_DIR:g" index.html
elif [[ ! -d gh_pages/refs-heads-master/ ]]; then
  DEFAULT_DIR=$BRANCH_DIR
  sed -i "s:__run_sign__:$RUN_SIGN:g" index.html
  sed -i "s:__branch_dir__:$DEFAULT_DIR:g" index.html
fi

info "updated index.html done."

info "updating executors.json"

sed -i "s:__run_sign__:$RUN_SIGN:g" executors.json
sed -i "s:__run_number__:$RUN_NUMBER:g" executors.json
sed -i "s:__run_id__:$RUN_ID:g" executors.json
sed -i "s:__branch_dir__:$BRANCH_DIR:g" executors.json

info "updated executors.json done."

cp index.html gh_pages/
cp executors.json gh_pages/$BRANCH_DIR/$RUN_SIGN/widgets/
cp environment.json gh_pages/$BRANCH_DIR/$RUN_SIGN/widgets/

mv gh_pages/ $sourcepath