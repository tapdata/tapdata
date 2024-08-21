package io.tapdata.pdk.cli;

import org.apache.commons.io.FilenameUtils;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static io.tapdata.pdk.cli.RegisterMain.AuthenticationType.*;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * <a href="https://picocli.info/">picocli.info</a>
 *
 * @author aplomb
 */
public class RegisterMain {
    private static final String BASE_PATH = basePath();

    private static final String TAG_JDBC = "jdbc";
    private static final String TAG_MQ = "mq";
    private static final String TAG_FILE = "file";

    enum AuthenticationType {
        GA,
        BETA,
        ALPHA,
    }

    /**
     * <ol>
     *     <li>
     *         <strong>Enum Names Rules:</strong>
     *         <ol>
     *             <li>Used the connector pom 'artifactId' property value</li>
     *             <li>Remove the '-connector' suffix</li>
     *             <li>Uppercase and replace '-' to '_'</li>
     *         </ol>
     *     </li>
     *     <li>
     *         <strong>Sort Rules:</strong>
     *         Refer to the sequence of connector engineering in IDEA
     *     </li>
     *     <li>
     *         <strong>Tag Rules:</strong>
     *     </li>
     * </ol>
     **/
    private enum ConnectorEnums {
        // EMPTY("empty"),
        ACTIVEMQ(ALPHA, TAG_MQ),
        AI_CHAT(BETA),
        ALI1688(BETA),
        ALIYUN_ADB_MYSQL(BETA, TAG_JDBC),
        ALIYUN_ADB_POSTGRES(BETA, TAG_JDBC),
        ALIYUN_MONGODB(BETA),
        ALIYUN_RDS_MARIADB(BETA, TAG_JDBC),
        ALIYUN_RDS_MYSQL(BETA),
        ALIYUN_RDS_POSTGRES(BETA, TAG_JDBC),
        AWS_RDS_MYSQL(ALPHA, TAG_JDBC),
        AZURE_COSMOSDB(BETA, TAG_JDBC),
        BES_CHANNELS(BETA),
        BIGQUERY(BETA),
        CLICKHOUSE(GA),
        CODING(BETA),
        CSV(GA, TAG_FILE),
        CUSTOM(GA),
        DATABEND(BETA, TAG_JDBC),
        DORIS(GA, TAG_JDBC),
        DUMMY(GA),
        DWS(BETA),
        ELASTICSEARCH(BETA),
        EXCEL(BETA, TAG_FILE),
        GITHUBCRM(BETA),
        GREENPLUM(ALPHA),
        HAZELCAST(ALPHA),
        HIVE1(BETA),
        HIVE3(BETA),
        HTTP_RECEIVER(BETA),
        HUAWEI_GAUSS_DB(GA, TAG_JDBC),
        HUBSPOT(BETA),
        HUDI(BETA),
        JSON(ALPHA, TAG_FILE),
        KAFKA(GA, TAG_MQ),
        LARK_APPROVAL(BETA),
        LARK_BITABLE(BETA),
        LARK_DOC(BETA),
        LARK_IM(BETA),
        LARK_TASK(BETA),
        MARIADB(GA, TAG_JDBC),
        METABASE(BETA),
        MOCK_SOURCE(GA),
        MOCK_TARGET(GA),
        MONGODB(GA),
        MONGODB_ATLAS(ALPHA),
        MONGODB_LOWER(ALPHA),
        MRS_HIVE3(BETA),
        MYSQL(GA, TAG_JDBC),
        MYSQL_PXC(BETA),
        OCEANBASE(GA, TAG_JDBC),
        OPENGAUSS(ALPHA, TAG_JDBC),
        POLAR_DB_MYSQL(BETA, TAG_JDBC),
        POLAR_DB_POSTGRES(BETA, TAG_JDBC),
        POSTGRES(GA, TAG_JDBC),
        QUICKAPI(ALPHA),
        RABBITMQ(ALPHA, TAG_MQ),
        REDIS(GA),
        ROCKETMQ(ALPHA),
        SALESFORCE(ALPHA),
        SELECTDB(BETA),
        SHEIN(BETA),
        STD_KAFKA(BETA, TAG_MQ),
        TABLESTORE(ALPHA),
        TDENGINE(BETA),
        TEMU(ALPHA),
        TENCENT_DB_MARIADB(ALPHA),
        TENCENT_DB_MONGODB(ALPHA),
        TENCENT_DB_POSTGRES(ALPHA),
        TIDB(GA, TAG_JDBC),
        VASTBASE(GA),
        VIKA(ALPHA),
        XML(ALPHA, TAG_FILE),
        YASHANDB(BETA, TAG_JDBC),
        ZOHO_CRM(BETA),
        ZOHO_DESK(BETA),
        ;

        private final String path;
        private final Set<String> tags = new HashSet<>();

        ConnectorEnums(AuthenticationType authType, String... tags) {
            String connectorPrefix = name().toLowerCase().replace("_", "-");
            this.path = String.format("%sconnectors/dist/%s-connector-v1.0-SNAPSHOT.jar", BASE_PATH, connectorPrefix);
            this.tags.add("all"); // add default tag: all
            this.tags.add(connectorPrefix); // add connector name tag
            this.tags.add(authType.name().toLowerCase()); // add connector AuthenticationType
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
        // -Dfilter=GA

        List<String> postList = new ArrayList<>();
        //String server = System.getProperty("server", "https://v3.test.cloud.tapdata.net/tm");
        String server = System.getProperty("server", "http://127.0.0.1:3000");
        String filter = System.getProperty("filter", "");
        //String server = System.getProperty("server", "http://192.168.1.189:30205");
        Collections.addAll(postList, "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-ak", "", "-sk", "", "-f", filter, "-t", server);
        String[] tags = System.getProperty("tags", "all").split(",");
        ConnectorEnums.addByTags(postList, tags);
        Main.registerCommands().execute(postList.toArray(new String[0]));
    }

    private static String basePath() {
        String connectorsHome = System.getProperty("connectors_home");
        if (null != connectorsHome) {
            return connectorsHome;
        }

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
