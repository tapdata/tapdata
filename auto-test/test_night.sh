# 1. 执行单测
p=`pwd`
cd $p/../
bash build/build.sh -c plugin-kit -m ut
build/build.sh -c file-storages -m ut
build/build.sh -c connectors-common -m ut
build/build.sh -c connectors -m ut
build/build.sh -c iengine -m ut

# 2. 执行集成测试
cd $p
rm -rf /tmp/fail
mkdir -p /tmp

if [[ "x"$CASE_CONFIG != "x" ]]; then
    echo $CASE_CONFIG|base64 -d > config.yaml
fi

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

cd cases || exit

rm -rf cases_result
for i in `ls|grep test_dev_sync_js.py|grep -v cases_result`; do
    rm -rf $i"_cases_result"
    python3 runner.py --case $i --bench 123 &> $i"_cases_result"
    cat $i"_cases_result" >> cases_result
done

pass=1

if [[ -f /tmp/fail ]]; then
    pass=0
fi
