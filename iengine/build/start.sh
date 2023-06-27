ulimit -c unlimited

sbin_file="ie.jar"
export app_type=DAAS
export backend_url=http://127.0.0.1:3000/api/
export TAPDATA_MONGO_URI='mongodb://127.0.0.1:27017/tapdata?authSource=admin'

touch logs/$sbin_file.log
nohup java -jar lib/$sbin_file &> logs/$sbin_file.log &
