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
python3 runner.py --case test_dev_sync.py --bench 123

if [[ -f /tmp/fail ]]; then
    echo "TEST FAIL !"
    exit 1
fi

echo "TEST PASS !"
