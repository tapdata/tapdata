ulimit -c unlimited

sbin_file="tapdata-agent.jar"
export app_type=DAAS
export backend_url=http://127.0.0.1:3000/api/
export TAPDATA_MONGO_URI='mongodb://127.0.0.1:27017/tapdata?authSource=admin'

mkdir -p logs/iengine && touch logs/iengine/$sbin_file.log
nohup java -jar components/$sbin_file &> logs/iengine/$sbin_file.log &
