package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TDDFactory {
    public static final String baseJarPath = "./connectors/dist/";
    public static final String baseConfPath = "tapdata-cli/src/main/resources/config/";
    private enum TddPath{
//        KINGBASE("kingbaser6-connector-v1.0-SNAPSHOT.jar","kingbase.json"),
//        OPENGAUSS("opengauss-connector-v1.0-SNAPSHOT.jar","opengauss.json"),
//        PGSQL("postgres-connector-v1.0-SNAPSHOT.jar","postgres.json"),
//        MySql("mysql-connector-v1.0-SNAPSHOT.jar","mysql.json"),
        YashanDB("yashandb-connector-v1.0-SNAPSHOT.jar","yashandb.json"),
//        PolarDBMySql("polar-db-mysql-connector-v1.0-SNAPSHOT.jar","polar-db-mysql.json"),
//        MongoDB("mongodb-connector-v1.0-SNAPSHOT.jar","mongodb.json"),
//        Coding("coding-connector-v1.0-SNAPSHOT.jar","coding.json"),
//        Tidb("tidb-connector-v1.0-SNAPSHOT.jar","tidb.json"),
//        MongoDBAtlas("mongodb-atlas-connector-v1.0-SNAPSHOT.jar","mongodb-atlas.json")
//        Coding("coding-connector-v1.0-SNAPSHOT.jar","coding.json"),
//        KafKa("kafka-connector-v1.0-SNAPSHOT.jar","kafka.json"),
//        ActiveMQ("activemq-connector-v1.0-SNAPSHOT.jar","activemq.json")
        ;
        String path;
        String conf;
        TddPath(String path,String conf){
            this.conf = conf;
            this.path = path;
        }
        public String getPath() {
            return path;
        }
        public void setPath(String path) {
            this.path = path;
        }
        public String getConf() {
            return conf;
        }
        public void setConf(String conf) {
            this.conf = conf;
        }
    }

    public static void main(String[] args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        CommonUtils.setProperty("TDD_AUTO_EXIT","0");
        //System.setProperty("is_debug_mode","true");
        args = new String[]{"test", "-c", null, null};
        for (TddPath tddJarPath : TddPath.values()) {
            args[2] = baseConfPath + tddJarPath.getConf();
            args[3] = baseJarPath + tddJarPath.getPath();
            Main.registerCommands().execute(args);
            try {
                TapConnectorManager instance = TapConnectorManager.getInstance();

                Class cla = TapConnectorManager.class;
                Field jarNameTapConnectorMap = cla.getDeclaredField("jarNameTapConnectorMap");
                jarNameTapConnectorMap.setAccessible(true);
                jarNameTapConnectorMap.set(instance,new ConcurrentHashMap<>());
                Field externalJarManager = cla.getDeclaredField("externalJarManager");
                externalJarManager.setAccessible(true);
                externalJarManager.set(instance,null);
                Field isStarted = cla.getDeclaredField("isStarted");
                isStarted.setAccessible(true);
                isStarted.set(instance,new AtomicBoolean(false));
            }catch (Exception e){
                throw new RuntimeException(e.getMessage());
            }
        }
        System.exit(0);
    }
}
