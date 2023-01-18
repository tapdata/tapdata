ulimit -c unlimited
sbin_file="ie.jar"

mvn clean install -DskipTests -P not_encrypt -U
if [[ $? -ne 0 ]]; then
    exit -1
fi

if [[ ! -f $sbin_file ]]; then
    exit -1
fi

rm -rf dist
mkdir -p dist dist/bin dist/lib dist/conf dist/logs
mv $sbin_file dist/lib
cp build/start.sh dist/bin
cp build/stop.sh dist/bin
cp build/status.sh dist/bin
