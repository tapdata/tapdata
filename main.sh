#! /bin/sh

cwd=`pwd`
enter_point=$cwd/common/utils/init
cd $enter_point

if [[ -d logs ]]; then
  mkdir logs
fi


python job.py --case test_merge --target mongodb --clean 2>&1>$cwd/logs/log