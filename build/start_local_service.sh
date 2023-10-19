rm -rf package && mkdir -p package
mv iengine/ie.jar package/ie.jar
mv manager/tm/target/tm-0.0.1-SNAPSHOT.jar package/tm.jar
mkdir -p package/etc/
cp -r auto-test/etc/* package/etc/
export mongodb_uri=${{ secrets.CI_MDB }}
export TAPDATA_MONGO_URI=${{ secrets.CI_MDB }}
nohup java -jar -Dserver.port=3030 -Dspring.data.mongodb.default.uri=${mongodb_uri} -Dspring.data.mongodb.log.uri=${mongodb_uri} -Dspring.data.mongodb.obs.uri=${mongodb_uri} -server -Xmx2G -Xms2G ./package/tm.jar --spring.config.additional-location=file:./package/etc/application-tm.yml &>> /dev/null &
while [[ 1 ]];
do
    sleep 5
    curl http://127.0.0.1:3030 &>> /dev/null
    if [[ $? -eq 0 ]]; then
        sleep 5
        break
    fi
done

nohup java -Dspring.config.location=file:../package/etc/application.yml -jar ../package/ie.jar &>> /dev/null &
sleep 10
