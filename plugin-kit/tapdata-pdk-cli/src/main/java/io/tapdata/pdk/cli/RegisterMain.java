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
        Empty(BASE_PATH + "connectors/dist/empty-connector-v1.1-SNAPSHOT.jar", false, "all", "empty"),
        Dummy(BASE_PATH + "connectors/dist/dummy-connector-v1.0-SNAPSHOT.jar", false, "all", "dummy", "basic"),
        Mysql(BASE_PATH + "connectors/dist/mysql-connector-v1.0-SNAPSHOT.jar", false, "all", "mysql", "basic", "jdbc"),
        Postgres(BASE_PATH + "connectors/dist/postgres-connector-v1.0-SNAPSHOT.jar", false, "all", "postgres", "basic", "jdbc"),
        Mongodb(BASE_PATH + "connectors/dist/mongodb-connector-v1.0-SNAPSHOT.jar", false, "all", "mongodb", "basic", "jdbc"),
        Elasticsearch(BASE_PATH + "connectors/dist/elasticsearch-connector-v1.0-SNAPSHOT.jar", false, "all", "elasticsearch"),
        Oceanbase(BASE_PATH + "connectors/dist/oceanbase-connector-v1.0-SNAPSHOT.jar", false, "all", "oceanbase"),
        Doris(BASE_PATH + "connectors/dist/doris-connector-v1.0-SNAPSHOT.jar", false, "all", "doris"),
        Activemq(BASE_PATH + "connectors/dist/activemq-connector-v1.0-SNAPSHOT.jar", false, "all", "activemq", "mq"),
        Rabbitmq(BASE_PATH + "connectors/dist/rabbitmq-connector-v1.0-SNAPSHOT.jar", false, "all", "rabbitmq", "mq"),
        Rocketmq(BASE_PATH + "connectors/dist/rocketmq-connector-v1.0-SNAPSHOT.jar", false, "all", "rocketmq", "mq"),
        Kafka(BASE_PATH + "connectors/dist/kafka-connector-v1.0-SNAPSHOT.jar", false, "all", "kafka", "mq"),
        Clickhouse(BASE_PATH + "connectors/dist/clickhouse-connector-v1.0-SNAPSHOT.jar", false, "all", "clickhouse"),
        Redis(BASE_PATH + "connectors/dist/redis-connector-v1.0-SNAPSHOT.jar", false, "all", "redis"),
        Hive1(BASE_PATH + "connectors/dist/hive1-connector-v1.0-SNAPSHOT.jar", false, "all", "hive1"),
        Mariadb(BASE_PATH + "connectors/dist/mariadb-connector-v1.0-SNAPSHOT.jar", false, "all", "mariadb"),
        Coding(BASE_PATH + "connectors/dist/coding-connector-v1.0-SNAPSHOT.jar", false, "all", "coding"),
        ZoHo(BASE_PATH + "connectors/dist/zoho-connector-v1.0-SNAPSHOT.jar", false, "all", "zoho"),
        Tidb(BASE_PATH + "connectors/dist/tidb-connector-v1.0-SNAPSHOT.jar", true, "all", "tidb"),
        Tablestore(BASE_PATH + "connectors/dist/tablestore-connector-v1.0-SNAPSHOT.jar", false, "all", "tablestore"),
        Custom(BASE_PATH + "connectors/dist/custom-connector-v1.0-SNAPSHOT.jar", false, "all", "custom"),
        Csv(BASE_PATH + "connectors/dist/csv-connector-v1.0-SNAPSHOT.jar", false, "all", "file"),
        Json(BASE_PATH + "connectors/dist/json-connector-v1.0-SNAPSHOT.jar", false, "all", "file"),
        Xml(BASE_PATH + "connectors/dist/xml-connector-v1.0-SNAPSHOT.jar", false, "all", "file"),
        Excel(BASE_PATH + "connectors/dist/excel-connector-v1.0-SNAPSHOT.jar", false, "all", "file"),
        BigQuery(BASE_PATH + "connectors/dist/bigquery-connector-v1.0-SNAPSHOT.jar", false, "all", "bigquery"),
        Vika(BASE_PATH + "connectors/dist/vika-connector-v1.0-SNAPSHOT.jar", false, "all", "vika"),
        ;

        private final String path;

        private boolean beta;
        private final Set<String> tags = new HashSet<>();

        ConnectorEnums(String path, boolean beta, String... tags) {
            this.path = path;
            if (null != tags) {
                this.tags.addAll(Arrays.asList(tags));
            }
            this.beta = beta;
        }

        public boolean contains(String... tags) {
            for (String s : tags) {
                if (this.tags.contains(s)) return true;
            }
            return false;
        }

        public static boolean addByTags(List<String> postList, String... tags) {
            boolean run = false;
            for (ConnectorEnums c : ConnectorEnums.values()) {
                if (c.contains(tags) && !c.beta) {
                    postList.add(c.path);
                    run = true;
                }
            }
            return run;
        }

        public static boolean addBetaByTags(List<String> postList, String... tags) {

            postList.add("-b");
            boolean run = false;
            for (ConnectorEnums c : ConnectorEnums.values()) {
                if (c.contains(tags) && c.beta) {
                    postList.add(c.path);
                    run = true;
                }
            }
            return run;
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
        List<String> postBetaList;
        //String server = System.getProperty("server", "https://v3.test.cloud.tapdata.net/tm");
        String server = System.getProperty("server", "http://localhost:3000");
        //String server = System.getProperty("server", "http://192.168.1.189:30205");
        Collections.addAll(postList, "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-ak", "", "-sk", "", "-t", server);

        postBetaList = new ArrayList<>(postList);
        String[] tags = System.getProperty("tags", "all").split(",");
        String beta = System.getProperty("beta", "false");
        boolean run = ConnectorEnums.addByTags(postList, tags);
        ConnectorEnums.addBetaByTags(postBetaList, tags);

        if (beta.equals("false") && run) {
            Main.registerCommands().execute(postList.toArray(new String[0]));
        } else {
            Main.registerCommands().execute(postBetaList.toArray(new String[0]));
        }
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
