# 1. 执行单测
OIFS=$IFS
rm -rf /tmp/xxx
mkdir -p /tmp/xxx
p=`pwd`
cd $p/../
ut_start_time=`date '+%s'`
bash build/build.sh -c plugin-kit -m ut | grep "Tests run" > /tmp/xxx/plugin-kit.ut
bash build/build.sh -c file-storages -m ut | grep "Tests run" > /tmp/xxx/file-storages.ut
bash build/build.sh -c connectors-common -m ut | grep "Tests run" > /tmp/xxx/connectors-common.ut
bash build/build.sh -c connectors -m ut | grep "Tests run" > /tmp/xxx/connectors.ut
bash build/build.sh -c iengine -m ut | grep "Tests run" > /tmp/xxx/iengine.ut
bash build/build.sh -c manager -m ut | grep "Tests run" > /tmp/xxx/manager.ut
ut_end_time=`date '+%s'`
ut_cost_time=`echo $ut_end_time-$ut_start_time|bc`

# 2. 执行集成测试
cd $p
rm -rf cases_result
rm -rf jobs_number
rm -rf pass_jobs_number
rm -rf /tmp/fail
mkdir -p /tmp

touch jobs_number
touch pass_jobs_number

if [[ "x"$CASE_CONFIG != "x" ]]; then
    echo $CASE_CONFIG|base64 -d > config.yaml
fi

start_result="成功"

server=`head -3 config.yaml |grep server|awk -F '"' '{print $2}'`
curl $server --max-time 3 2>&1 > /dev/null
if [[ $? -ne 0 ]]; then
    echo "curl server timeout, skip testing..."
    echo "tapdata server may not work ok, please check it"
    start_result="失败"
fi

curl $server --max-time 3 -v 2>&1|grep "HTTP/1"|grep 200
if [[ $? -ne 0 ]]; then
    echo "curl server not return 200, skip tesing..."
    echo "tapdata server UI may not work ok, will skip it"
fi

python3 init/prepare_data.py
python3 init/create_datasource.py
python3 init/create_datasource.py

data_set="["
for i in `ls init/data/|grep "\.py"|grep -v grep`; do
    if [[ $data_set == "[" ]]; then
        data_set=$data_set`echo $i|awk -F '.' '{print $1}'`
    else
        data_set=$data_set", "`echo $i|awk -F '.' '{print $1}'`
    fi
done
data_set=$data_set"]"
echo "std::out >> 环境准备: 用例执行编号为: "`cat init/.table_suffix_cache_file`", 导入数据集为: ${data_set}" >> cases/cases_result

connectors="["
for i in `cat config.yaml|grep connector|awk -F ":" '{print $2}'|awk -F '"' '{print $2}'`; do
    if [[ $connectors == "[" ]]; then
        connectors=$connectors$i
    else
        connectors=$connectors","$i
    fi
done
bench=123
connectors=$connectors"]"
echo "std::out >> 用例: 创建数据源, 并加载模型, 数据源类型包括: $connectors, 此次集成测试增量数据量为: $bench" >> cases/cases_result

cd cases

it_start_time=`date '+%s'`
cases=`ls|grep test_|grep -v cases_result`
echo "$cases"
ls -al
IFS=$'\n'
for i in $cases; do
    echo "will exec case: "$i
    rm -rf $i"_cases_result"
    python3 runner.py --case $i --bench $bench &> $i"_cases_result"
    cat $i"_cases_result"
    cat $i"_cases_result" >> cases_result
done
IFS=$OIFS
it_end_time=`date '+%s'`
it_cost_time=`echo $it_end_time-$it_start_time|bc`

cat cases_result

pass="true"

if [[ -f /tmp/fail ]]; then
    pass="false"
fi

if [[ $start_result != "成功" ]]; then
    pass="false"
fi

cd $p

case_results=""
IFS=$'\n'
x=1
for i in `cat cases/cases_result|grep "std::out"`; do
    r=`echo $i|awk -F "std::out >> " '{print $2}'`
    if [[ "x"$case_results == "x" ]]; then
        case_results='"'$x'. '$r'"'
    else
        case_results=$case_results',"'$x'. '$r'"'
    fi
    x=`echo $x+1|bc`
done
IFS=$OIFS

jobs_number=`wc -l cases/jobs_number|awk '{print $1}'`
pass_jobs_number=`wc -l cases/pass_jobs_number|awk '{print $1}'`

plugin_kit_ut=`cat /tmp/xxx/plugin-kit.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
file_storages_ut=`cat /tmp/xxx/file-storages.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
connectors_common_ut=`cat /tmp/xxx/connectors-common.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
connectors_ut=`cat /tmp/xxx/connectors.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
iengine_ut=`cat /tmp/xxx/iengine.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
manager_ut=`cat /tmp/xxx/manager.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`

ut_sum=`cat /tmp/xxx/*|grep "Tests run"|grep -v "Time elapsed"|awk -F "Tests run:" '{print $2}'|awk -F "," '{print $1}'|awk '{s+=$1} END {print s}'`
ut_failure=`cat /tmp/xxx/*|grep "Tests run"|grep -v "Time elapsed"|awk -F "Failures:" '{print $2}'|awk -F "," '{print $1}'|awk '{s+=$1} END {print s}'`
ut_error=`cat /tmp/xxx/*|grep "Tests run"|grep -v "Time elapsed"|awk -F "Errors:" '{print $2}'|awk -F "," '{print $1}'|awk '{s+=$1} END {print s}'`
ut_skip=`cat /tmp/xxx/*|grep "Tests run"|grep -v "Time elapsed"|awk -F "Skipped:" '{print $2}'|awk -F "," '{print $1}'|awk '{s+=$1} END {print s}'`
ut_pass=`echo $ut_sum-$ut_failure-$ut_error-$ut_skip|bc`

not_success_its=`cat /tmp/xxx/*|grep --color=never "Time elapsed"|grep --color=never -v "Failures: 0, Errors: 0, Skipped: 0"`
IFS=$'\n'
uts=""
for i in $not_success_its; do
    if [[ "x"$uts == "x" ]]; then
        uts='"'$i'"'
    else
        uts=$uts',"'$i'"'
    fi
done
IFS=$OIFS

pip3 install argparse GitPython psutil

env

if [[ "x"$jobs_number == "x" ]]; then
    jobs_number="0"
fi

if [[ "x"$pass_jobs_number == "x" ]]; then
    pass_jobs_number="0"
fi

if [[ "x"$jobs_number != "x"$pass_jobs_number ]]; then
    pass="false"
fi

message='{"ut_cost_time":'$ut_cost_time',"it_cost_time":'$it_cost_time',"pass":'$pass',"ut_sum":'$ut_sum',"ut_pass":'$ut_pass',"it_sum":'$jobs_number',"it_pass":'$pass_jobs_number',"build_result":"通过","start_result":"'$start_result'","its":['$case_results']}'

echo $message

python3 ../build/feishu_notice.py --branch $CURRENT_BRANCH --runner "OP 版本每夜自动化测试" --detail_url "${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}" --token ${GITHUB_TOKEN} --job_id ${GITHUB_RUN_ID} --app_id ${FEISHU_APP_ID} --person_in_charge ${FEISHU_PERSON_IN_CHARGE} --app_secret ${FEISHU_APP_SECRET} --chat_id oc_79ef2aafcf9a712bfc31280f80498732 --message_type night_build_notice --message "$message"
