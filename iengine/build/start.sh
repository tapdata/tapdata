ulimit -c unlimited

sbin_file="tapdata-agent.jar"
export app_type=DAAS
tm_port=${tm_port:-"3030"}
export backend_url=http://127.0.0.1:$tm_port/api/
if [[ -z $MONGO_URI ]]; then
  MONGO_URI="mongodb://127.0.0.1:27017/tapdata"
fi
export TAPDATA_MONGO_URI=$MONGO_URI

mkdir -p logs/iengine && touch logs/iengine/$sbin_file.log
nohup java -jar components/$sbin_file &> logs/iengine/$sbin_file.log &
