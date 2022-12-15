package io.tapdata.pdk.cli;

import org.apache.commons.io.FilenameUtils;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class RegisterMain {
    private static final String BASE_PATH = basePath();

    private enum ConnectorEnums {
//        Empty(BASE_PATH + "connectors/dist/empty-connector-v1.1-SNAPSHOT.jar", "all", "empty"),
        Dummy(BASE_PATH + "connectors/dist/dummy-connector-v1.0-SNAPSHOT.jar", "all", "dummy", "basic"),
        Mysql(BASE_PATH + "connectors/dist/mysql-connector-v1.0-SNAPSHOT.jar", "all", "mysql", "basic", "jdbc"),
        Postgres(BASE_PATH + "connectors/dist/postgres-connector-v1.0-SNAPSHOT.jar", "all", "postgres", "basic", "jdbc"),
        Mongodb(BASE_PATH + "connectors/dist/mongodb-connector-v1.0-SNAPSHOT.jar", "all", "mongodb", "basic", "jdbc"),
        Elasticsearch(BASE_PATH + "connectors/dist/elasticsearch-connector-v1.0-SNAPSHOT.jar", "all", "elasticsearch"),
        Oceanbase(BASE_PATH + "connectors/dist/oceanbase-connector-v1.0-SNAPSHOT.jar", "all", "oceanbase"),
        Doris(BASE_PATH + "connectors/dist/doris-connector-v1.0-SNAPSHOT.jar", "all", "doris"),
        Activemq(BASE_PATH + "connectors/dist/activemq-connector-v1.0-SNAPSHOT.jar", "all", "activemq", "mq"),
        Rabbitmq(BASE_PATH + "connectors/dist/rabbitmq-connector-v1.0-SNAPSHOT.jar", "all", "rabbitmq", "mq"),
        Rocketmq(BASE_PATH + "connectors/dist/rocketmq-connector-v1.0-SNAPSHOT.jar", "all", "rocketmq", "mq"),
        Kafka(BASE_PATH + "connectors/dist/kafka-connector-v1.0-SNAPSHOT.jar", "all", "kafka", "mq"),
        Clickhouse(BASE_PATH + "connectors/dist/clickhouse-connector-v1.0-SNAPSHOT.jar", "all", "clickhouse"),
        Redis(BASE_PATH + "connectors/dist/redis-connector-v1.0-SNAPSHOT.jar", "all", "redis"),
        Hive1(BASE_PATH + "connectors/dist/hive1-connector-v1.0-SNAPSHOT.jar", "all", "hive1"),
        Mariadb(BASE_PATH + "connectors/dist/mariadb-connector-v1.0-SNAPSHOT.jar", "all", "mariadb"),
        Coding(BASE_PATH + "connectors/dist/coding-connector-v1.0-SNAPSHOT.jar", "all", "coding"),
        ZoHo(BASE_PATH + "connectors/dist/zoho-connector-v1.0-SNAPSHOT.jar", "all", "zoho"),
        Tidb(BASE_PATH + "connectors/dist/tidb-connector-v1.0-SNAPSHOT.jar", "all", "tidb"),
        Tablestore(BASE_PATH + "connectors/dist/tablestore-connector-v1.0-SNAPSHOT.jar", "all", "tablestore"),
        Custom(BASE_PATH + "connectors/dist/custom-connector-v1.0-SNAPSHOT.jar", "all", "custom"),
        Csv(BASE_PATH + "connectors/dist/csv-connector-v1.0-SNAPSHOT.jar", "all", "file"),
        Json(BASE_PATH + "connectors/dist/json-connector-v1.0-SNAPSHOT.jar", "all", "file"),
        Xml(BASE_PATH + "connectors/dist/xml-connector-v1.0-SNAPSHOT.jar", "all", "file"),
        Excel(BASE_PATH + "connectors/dist/excel-connector-v1.0-SNAPSHOT.jar", "all", "file"),
        BigQuery(BASE_PATH + "connectors/dist/bigquery-connector-v1.0-SNAPSHOT.jar", "all", "bigquery"),
        Vika(BASE_PATH + "connectors/dist/vika-connector-v1.0-SNAPSHOT.jar", "all", "vika"),
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
        // VM options samples:
        // -Dtags=all -Dserver=http://localhost:3000
        // -Dtags=dummy,mysql
        // -Dserver=http://192.168.1.132:31966
        // -Dserver=http://192.168.1.132:31787
        // -Dserver=http://192.168.1.181:31321
        // -Dbeta=true

        List<String> postList = new ArrayList<>();
        //String server = System.getProperty("server", "https://v3.test.cloud.tapdata.net/tm");
        String server = System.getProperty("server", "http://localhost:3000");
        //String server = System.getProperty("server", "http://192.168.1.189:30205");
        Collections.addAll(postList, "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-ak", "", "-sk", "", "-t", server);

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
            Path path = Paths.get(resource.getPath() + "../../../../");
            String basePath = path.toFile().getCanonicalPath() + "/";
            System.out.println("basePath:" + basePath);
            return basePath;
        } catch (Throwable throwable) {
            return FilenameUtils.concat(resource.getPath(), "../../../../");
        }

    }
}
