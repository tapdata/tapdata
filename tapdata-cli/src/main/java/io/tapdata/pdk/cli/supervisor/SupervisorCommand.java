package io.tapdata.pdk.cli.supervisor;

import io.tapdata.pdk.cli.RegisterMain;
import io.tapdata.pdk.cli.Main;
import org.apache.commons.io.FilenameUtils;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SupervisorCommand {
    private static final String BASE_PATH = basePath();

    private enum ConnectorEnums {
        //Empty(BASE_PATH + "connectors/dist/empty-connector-v1.1-SNAPSHOT.jar", "all", "empty"),
//        Dummy(BASE_PATH + "connectors/dist/dummy-connector-v1.0-SNAPSHOT.jar", "all", "dummy", "basic"),
        Mysql(BASE_PATH + "connectors/dist/mysql-connector-v1.0-SNAPSHOT.jar", "all", "mysql", "basic", "jdbc"),
//        Postgres(BASE_PATH + "connectors/dist/postgres-connector-v1.0-SNAPSHOT.jar", "all", "postgres", "basic", "jdbc"),
//        Mongodb(BASE_PATH + "connectors/dist/mongodb-connector-v1.0-SNAPSHOT.jar", "all", "mongodb", "basic", "jdbc"),
//        Elasticsearch(BASE_PATH + "connectors/dist/elasticsearch-connector-v1.0-SNAPSHOT.jar", "all", "elasticsearch"),
//        Oceanbase(BASE_PATH + "connectors/dist/oceanbase-connector-v1.0-SNAPSHOT.jar", "all", "oceanbase"),
//        Doris(BASE_PATH + "connectors/dist/doris-connector-v1.0-SNAPSHOT.jar", "all", "doris"),
//        Activemq(BASE_PATH + "/connectors/dist/activemq-connector-v1.0-SNAPSHOT.jar", "all", "activemq", "mq"),
//        Rabbitmq(BASE_PATH + "connectors/dist/rabbitmq-connector-v1.0-SNAPSHOT.jar", "all", "rabbitmq", "mq"),
//        Rocketmq(BASE_PATH + "connectors/dist/rocketmq-connector-v1.0-SNAPSHOT.jar", "all", "rocketmq", "mq"),
//        Kafka(BASE_PATH + "connectors/dist/kafka-connector-v1.0-SNAPSHOT.jar", "all", "kafka", "mq"),
//        Clickhouse(BASE_PATH + "connectors/dist/clickhouse-connector-v1.0-SNAPSHOT.jar", "all", "clickhouse"),
//        Redis(BASE_PATH + "connectors/dist/redis-connector-v1.0-SNAPSHOT.jar", "all", "redis"),
//        Hive1(BASE_PATH + "connectors/dist/hive1-connector-v1.0-SNAPSHOT.jar", "all", "hive1"),
//        Mariadb(BASE_PATH + "connectors/dist/mariadb-connector-v1.0-SNAPSHOT.jar", "all", "mariadb"),
//        Coding(BASE_PATH + "connectors/dist/coding-connector-v1.0-SNAPSHOT.jar", "all", "coding"),
//        ZOHODesk(BASE_PATH + "connectors/dist/zoho-desk-connector-v1.0-SNAPSHOT.jar", "all", "zoho-desk"),
//        Tidb(BASE_PATH + "connectors/dist/tidb-connector-v1.0-SNAPSHOT.jar", "all", "tidb"),
//        Tablestore(BASE_PATH + "connectors/dist/tablestore-connector-v1.0-SNAPSHOT.jar", "all", "tablestore"),
//        Custom(BASE_PATH + "connectors/dist/custom-connector-v1.0-SNAPSHOT.jar", "all", "custom"),
//        Csv(BASE_PATH + "connectors/dist/csv-connector-v1.0-SNAPSHOT.jar", "all", "file"),
//        Json(BASE_PATH + "connectors/dist/json-connector-v1.0-SNAPSHOT.jar", "all", "file"),
//        Xml(BASE_PATH + "connectors/dist/xml-connector-v1.0-SNAPSHOT.jar", "all", "file"),
//        Excel(BASE_PATH + "connectors/dist/excel-connector-v1.0-SNAPSHOT.jar", "all", "file"),
//        BigQuery(BASE_PATH + "connectors/dist/bigquery-connector-v1.0-SNAPSHOT.jar", "all", "bigquery"),
//        Vika(BASE_PATH + "connectors/dist/vika-connector-v1.0-SNAPSHOT.jar", "all", "vika"),
//        TDengine(BASE_PATH + "connectors/dist/tdengine-connector-v1.0-SNAPSHOT.jar", "all", "tdengine"),
//        QuickApi(BASE_PATH + "connectors/dist/quickapi-connector-v1.0-SNAPSHOT.jar", "all", "quickapi"),
//        Aliyun_ADB_MYSQL(BASE_PATH + "connectors/dist/aliyun-adb-mysql-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-adb-mysql"),
//        Aliyun_ADB_POSTGRES(BASE_PATH + "connectors/dist/aliyun-adb-postgres-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-adb-postgres"),
//        Aliyun_MONGODB(BASE_PATH + "connectors/dist/aliyun-mongodb-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-db-mongodb"),
//        Aliyun_RDS_MARIADB(BASE_PATH + "connectors/dist/aliyun-rds-mariadb-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-rds-mariadb"),
//        Aliyun_RDS_MYSQL(BASE_PATH + "connectors/dist/aliyun-rds-mysql-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-rds-mysql"),
//        Aliyun_RDS_POSTGRES(BASE_PATH + "connectors/dist/aliyun-rds-postgres-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-rds-postgres"),
//        AWS_RDS_MYSQL(BASE_PATH + "connectors/dist/aws-rds-mysql-connector-v1.0-SNAPSHOT.jar", "all", "aws-rds-mysql"),
//        MYSQL_PXC(BASE_PATH + "connectors/dist/mysql-pxc-connector-v1.0-SNAPSHOT.jar", "all", "mysql-pxc"),
//        POLAR_DB_MYSQL(BASE_PATH + "connectors/dist/polar-db-mysql-connector-v1.0-SNAPSHOT.jar", "all", "polar-db-mysql"),
//        POLAR_DB_POSTGRES(BASE_PATH + "connectors/dist/polar-db-postgres-connector-v1.0-SNAPSHOT.jar", "all", "polar-db-postgres"),
//        TENCENT_DB_MYSQL(BASE_PATH + "connectors/dist/tencent-db-mysql-connector-v1.0-SNAPSHOT.jar", "all", "tencent-db-mysql"),
//        TENCENT_DB_MARIADB(BASE_PATH + "connectors/dist/tencent-db-mariadb-connector-v1.0-SNAPSHOT.jar", "all", "tencent-db-mariadb"),
//        TENCENT_DB_MONGODB(BASE_PATH + "connectors/dist/tencent-db-mongodb-connector-v1.0-SNAPSHOT.jar", "all", "tencent-db-mongodb"),
//        TENCENT_DB_POSTGRES(BASE_PATH + "connectors/dist/tencent-db-postgres-connector-v1.0-SNAPSHOT.jar", "all", "tencent-db-postgres"),
//        SelectDB(BASE_PATH + "connectors/dist/selectdb-connector-v1.0-SNAPSHOT.jar", "all", "selectdb"),
//        Metabase(BASE_PATH + "connectors/dist/metabase-connector-v1.0-SNAPSHOT.jar", "all", "metabase"),
//        LarkIM(BASE_PATH + "connectors/dist/lark-im-connector-v1.0-SNAPSHOT.jar", "all", "lark-im"),
//        Databend(BASE_PATH + "connectors/dist/databend-connector-v1.0-SNAPSHOT.jar", "all", "databend"),
//        Hazelcast(BASE_PATH + "connectors/dist/hazelcast-connector-v1.0-SNAPSHOT.jar", "all", "hazelcast"),
//        ZohoCRM(BASE_PATH + "connectors/dist/zoho-crm-connector-v1.0-SNAPSHOT.jar", "all", "zoho-crm"),
//        LarkTask(BASE_PATH + "connectors/dist/lark-task-connector-v1.0-SNAPSHOT.jar", "all", "lark-task"),
//        OpenGauss(BASE_PATH + "connectors/dist/openGauss-connector-v1.0-SNAPSHOT.jar", "all", "open-gauss", "basic", "jdbc"),

        ;

        private final String path;
        private final Set<String> tags = new HashSet<>();

        ConnectorEnums(String path, String... tags) {
            this.path = path;
            if (null != tags) {
                this.tags.addAll(Arrays.asList(tags));
            }
        }

        public boolean contains(String... tags) {
            for (String s : tags) {
                if (this.tags.contains(s)) return true;
            }
            return false;
        }

        public static void addByTags(List<String> postList, String... tags) {
            for (ConnectorEnums c : ConnectorEnums.values()) {
                if (c.contains(tags)) {
                    postList.add(c.path);
                }
            }
        }
    }

    public static void main(String... args) {
        List<String> postList = new ArrayList<>();
        Collections.addAll(postList, "supervisor");
        String[] tags = System.getProperty("tags", "all").split(",");
        ConnectorEnums.addByTags(postList, tags);
        Main.registerCommands().execute(postList.toArray(new String[0]));
    }
    private static String basePath() {
        URL resource = RegisterMain.class.getClassLoader().getResource("");
        if (null == resource) {
            return "/";
        }

        try {
            Path path = Paths.get(resource.getPath() + "../../../");
            String basePath = path.toFile().getCanonicalPath() + "/";
            System.out.println("basePath:" + basePath);
            return basePath;
        } catch (Throwable throwable) {
            return FilenameUtils.concat(resource.getPath(), "../../../");
        }

    }
}
