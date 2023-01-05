OIFS=$IFS
rm -rf /tmp/xxx
mkdir -p /tmp/xxx
p=`pwd`

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

echo "待测试环境地址为: ${ADDR}"

sed "s|127.0.0.1:3000|${ADDR}|g" config.yaml &> config2.yaml
mv config2.yaml config.yaml

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
cases=`ls|grep test_|grep -v cases_result|grep -v widetable`
echo "$cases"
ls -al
IFS=$'\n'
for i in $cases; do
    echo "will exec case: "$i
    rm -rf $i"_cases_result"
    python3 runner.py --smart_cdc --case $i --bench $bench &> $i"_cases_result"
    cat $i"_cases_result"
    cat $i"_cases_result" >> cases_result
done
IFS=$OIFS

python3 runner.py --source mongodb --sink mongodb --case test_widetable_check.py &> wide_table_cases_result
cat wide_table_cases_result >> cases_result

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

ut_cost_time=0
ut_sum=0
ut_pass=0

message='{"ut_cost_time":'$ut_cost_time',"it_cost_time":'$it_cost_time',"pass":'$pass',"ut_sum":'$ut_sum',"ut_pass":'$ut_pass',"it_sum":'$jobs_number',"it_pass":'$pass_jobs_number',"build_result":"通过","start_result":"'$start_result'","its":['$case_results']}'

echo $message
if [[ "x"$FEISHU_CHAT_ID == "x" ]]; then
    FEISHU_CHAT_ID="oc_79ef2aafcf9a712bfc31280f80498732"
fi
echo "feishu chat id is: ${FEISHU_CHAT_ID}"

python3 ../build/feishu_notice.py --branch "test" --addr $ADDR --runner "OP 环境测一测" --detail_url "${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}" --token ${GITHUB_TOKEN} --job_id ${GITHUB_RUN_ID} --app_id ${FEISHU_APP_ID} --person_in_charge ${FEISHU_PERSON_IN_CHARGE} --app_secret ${FEISHU_APP_SECRET} --chat_id ${FEISHU_CHAT_ID} --message_type night_build_notice --message "$message"
