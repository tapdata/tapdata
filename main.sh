#! /bin/bash

#cwd=/mnt/d/automated_test
cwd=~/automated_test
#python_home=/usr/local/lib/python-linux-venvs/automated-test-venv/bin
python_home=/home/python3/virtualenvs/automated_test/bin
enter_point=$cwd/common/utils/init
cd $enter_point


if [[ -d $cwd/logs ]]; then
    $python_home/python job.py --case test_sync --source dummy --target dummy --clean &> $cwd/logs/log;
    $python_home/python job.py --case test_js --source dummy --target dummy --clean &>> $cwd/logs/log
#         python job.py --case test_merge --source dummy --target mongodb --clean &>> $cwd/logs/log
else
    mkdir $cwd/logs &&
    $python_home/python job.py --case test_sync --source dummy --target dummy --clean &> $cwd/logs/log;
    $python_home/python job.py --case test_js --source dummy --target dummy --clean &>> $cwd/logs/log
#         python job.py --case test_merge --source dummy --target mongodb --clean &>> $cwd/logs/log
fi