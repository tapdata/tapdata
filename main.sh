#! /bin/bash

cwd=`pwd`
enter_point=$cwd/common/utils/init
cd $enter_point

if [[ -d $cwd/logs ]]; then
    python job.py --case test_merge --source mysql --target mongodb --clean 2>&1>$cwd/logs/log
else
    mkdir $cwd/logs && python job.py --case test_merge --source mysql --target mongodb --clean 2>&1>$cwd/logs/log
fi