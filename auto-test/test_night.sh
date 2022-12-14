# 1. 执行单测
rm -rf /tmp/xxx
mkdir -p /tmp/xxx
p=`pwd`
cd $p/../
bash build/build.sh -c plugin-kit -m ut | grep "Tests run" > /tmp/xxx/plugin-kit.ut
bash build/build.sh -c file-storages -m ut | grep "Tests run" > /tmp/xxx/file-storages.ut
bash build/build.sh -c connectors-common -m ut | grep "Tests run" > /tmp/xxx/connectors-common.ut
bash build/build.sh -c connectors -m ut | grep "Tests run" > /tmp/xxx/connectors.ut
bash build/build.sh -c iengine -m ut | grep "Tests run" > /tmp/xxx/iengine.ut
bash build/build.sh -c manager -m ut | grep "Tests run" > /tmp/xxx/manager.ut

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

cat config.yaml

server=`head -3 config.yaml |grep server|awk -F '"' '{print $2}'`
curl $server --max-time 3 2>&1 > /dev/null
if [[ $? -ne 0 ]]; then
    echo "curl server timeout, skip testing..."
    echo "tapdata server may not work ok, please check it"
    echo "TEST FAIL !"
    exit 1
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
    data_set=$data_set", "`echo $i|awk -F '.' '{print $1}'`
done
data_set=$data_set"]"
echo "std::out >> 环境准备: 用例执行编号为:"`cat init/.table_suffix_cache_file`", 导入数据集为: ${data_set}" >> cases/cases_result

connectors="["
for i in `cat config.yaml|grep connector|awk -F ":" '{print $2}'`; do
    connectors=$connectors","$i
done
connectors=$connectors"]"
echo $connectors
echo "std::out >> 用例: 创建数据源, 并加载模型, 数据源类型包括: $connectos" >> cases/cases_result

cd cases

for i in `ls|grep test_dev_sync.py|grep -v cases_result`; do
    rm -rf $i"_cases_result"
    python3 runner.py --case $i --bench 123 &> $i"_cases_result"
    cat $i"_cases_result" >> cases_result
done

cat cases_result

pass="true"

if [[ -f /tmp/fail ]]; then
    pass="false"
fi

cd $p

case_results=""
for i in `cat cases/cases_result|grep "std::out"`; do
    case_results=$case_results'"'$i'",'
done

jobs_number=`wc -l cases/jobs_number|awk '{print $1}'`
pass_jobs_number=`wc -l cases/pass_jobs_number|awk '{print $1}'`
echo "jobs number is: $jobs_number"
echo "pass jobs number is: $pass_jobs_number"

plugin_kit_ut=`cat /tmp/xxx/plugin-kit.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
file_storages_ut=`cat /tmp/xxx/file-storages.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
connectors_common_ut=`cat /tmp/xxx/connectors-common.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
connectors_ut=`cat /tmp/xxx/connectors.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
iengine_ut=`cat /tmp/xxx/iengine.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`
manager_ut=`cat /tmp/xxx/manager.ut|grep "Tests run"|grep -v "\-\-"|grep -v "Tests run: 0"`

echo $case_results

echo $manager_ut

echo $iengine_ut

ut_sum=`cat /tmp/xxx/*|grep "Tests run"|grep -v "Time elapsed"|awk -F "Tests run:" '{print $2}'|awk -F "," '{print $1}'|awk '{s+=$1} END {print s}'`
ut_failure=`cat /tmp/xxx/*|grep "Tests run"|grep -v "Time elapsed"|awk -F "Failures:" '{print $2}'|awk -F "," '{print $1}'|awk '{s+=$1} END {print s}'`
ut_error=`cat /tmp/xxx/*|grep "Tests run"|grep -v "Time elapsed"|awk -F "Errors:" '{print $2}'|awk -F "," '{print $1}'|awk '{s+=$1} END {print s}'`
ut_skip=`cat /tmp/xxx/*|grep "Tests run"|grep -v "Time elapsed"|awk -F "Skipped:" '{print $2}'|awk -F "," '{print $1}'|awk '{s+=$1} END {print s}'`
ut_pass=`echo $it_sum-$it_failure-$it_error-$it_skip|bc`

echo "ut sum number is: $ut_sum, pass number is: $ut_pass"

OIFS=$IFS
not_success_its=`cat /tmp/xxx|grep "Time elapsed"|grep -v "Failures: 0, Errors: 0, Skipped: 0"`
IFS=$'\n'
its=""
for i in $not_success_its; do
    its='"'$i'",'$its
done

pip3 install argparse, git

env

message='{"pass":'$pass',"ut_sum":'$ut_sum',"ut_pass":'$ut_pass',"it_sum":'$jobs_number',"it_pass":'$pass_jobs_number',"build_result":"通过","start_result":"成功","its":['$case_results']}'
echo $message

python3 ../build/feishu_notice.py --branch $CURRENT_BRANCH --runner "OP 版本每夜自动化测试" --detail_url "${server_url}/${repository}/actions/runs/${run_id}" --token ${GITHUB_TOKEN} --job_id ${run_id} --app_id ${FEISHU_APP_ID} --person_in_charge ${FEISHU_PERSON_IN_CHARGE} --app_secret ${FEISHU_APP_SECRET} --chat_id gf9b5g97 --message_type night_build_notice --message $message
