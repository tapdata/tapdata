package io.tapdata.pdk.cli;

import org.apache.commons.io.FilenameUtils;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class RegisterMain {
	private static final String BASE_PATH = basePath();

	private enum ConnectorEnums {
		// Empty(BASE_PATH + "connectors/dist/empty-connector-v1.1-SNAPSHOT.jar", "all", "empty"),
		Dummy(BASE_PATH + "connectors/dist/dummy-connector-v1.0-SNAPSHOT.jar", "all", "dummy", "basic"),
		Mock_Source(BASE_PATH + "connectors/dist/mock-source-connector-v1.0-SNAPSHOT.jar", "all", "mock-source", "basic"),
		Mock_Target(BASE_PATH + "connectors/dist/mock-target-connector-v1.0-SNAPSHOT.jar", "all", "mock-target", "basic"),
		Mysql(BASE_PATH + "connectors/dist/mysql-connector-v1.0-SNAPSHOT.jar", "all", "mysql", "basic", "jdbc"),
		Postgres(BASE_PATH + "connectors/dist/postgres-connector-v1.0-SNAPSHOT.jar", "all", "postgres", "basic", "jdbc"),
		Dws(BASE_PATH + "connectors/dist/dws-connector-v1.0-SNAPSHOT.jar", "all", "dws", "basic", "jdbc"),
		Mongodb(BASE_PATH + "connectors/dist/mongodb-connector-v1.0-SNAPSHOT.jar", "all", "mongodb", "basic", "jdbc"),
		Mongodb_Atlas(BASE_PATH + "connectors/dist/mongodb-atlas-connector-v1.0-SNAPSHOT.jar", "all", "mongodb-atlas"),
		Mongodb_Lower(BASE_PATH + "connectors/dist/mongodb-lower-connector-v1.0-SNAPSHOT.jar", "all", "mongodb-lower"),
		Elasticsearch(BASE_PATH + "connectors/dist/elasticsearch-connector-v1.0-SNAPSHOT.jar", "all", "elasticsearch"),
		Oceanbase(BASE_PATH + "connectors/dist/oceanbase-mysql-connector-v1.0-SNAPSHOT.jar", "all", "oceanbase"),
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
		ZOHODesk(BASE_PATH + "connectors/dist/zoho-desk-connector-v1.0-SNAPSHOT.jar", "all", "zoho-desk"),
		Tidb(BASE_PATH + "connectors/dist/tidb-connector-v1.0-SNAPSHOT.jar", "all", "tidb"),
		Tablestore(BASE_PATH + "connectors/dist/tablestore-connector-v1.0-SNAPSHOT.jar", "all", "tablestore"),
		Custom(BASE_PATH + "connectors/dist/custom-connector-v1.0-SNAPSHOT.jar", "all", "custom"),
		Csv(BASE_PATH + "connectors/dist/csv-connector-v1.0-SNAPSHOT.jar", "all", "file"),
		Json(BASE_PATH + "connectors/dist/json-connector-v1.0-SNAPSHOT.jar", "all", "file"),
		Xml(BASE_PATH + "connectors/dist/xml-connector-v1.0-SNAPSHOT.jar", "all", "file"),
		Excel(BASE_PATH + "connectors/dist/excel-connector-v1.0-SNAPSHOT.jar", "all", "file"),
		BigQuery(BASE_PATH + "connectors/dist/bigquery-connector-v1.0-SNAPSHOT.jar", "all", "bigquery"),
		Vika(BASE_PATH + "connectors/dist/vika-connector-v1.0-SNAPSHOT.jar", "all", "vika"),
		TDengine(BASE_PATH + "connectors/dist/tdengine-connector-v1.0-SNAPSHOT.jar", "all", "tdengine"),
		QuickApi(BASE_PATH + "connectors/dist/quickapi-connector-v1.0-SNAPSHOT.jar", "all", "quickapi"),
		Aliyun_ADB_MYSQL(BASE_PATH + "connectors/dist/aliyun-adb-mysql-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-adb-mysql"),
		Aliyun_ADB_POSTGRES(BASE_PATH + "connectors/dist/aliyun-adb-postgres-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-adb-postgres"),
		Aliyun_MONGODB(BASE_PATH + "connectors/dist/aliyun-mongodb-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-db-mongodb"),
		Aliyun_RDS_MARIADB(BASE_PATH + "connectors/dist/aliyun-rds-mariadb-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-rds-mariadb"),
		Aliyun_RDS_MYSQL(BASE_PATH + "connectors/dist/aliyun-rds-mysql-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-rds-mysql"),
		Aliyun_RDS_POSTGRES(BASE_PATH + "connectors/dist/aliyun-rds-postgres-connector-v1.0-SNAPSHOT.jar", "all", "aliyun-rds-postgres"),
		AWS_RDS_MYSQL(BASE_PATH + "connectors/dist/aws-rds-mysql-connector-v1.0-SNAPSHOT.jar", "all", "aws-rds-mysql"),
		MYSQL_PXC(BASE_PATH + "connectors/dist/mysql-pxc-connector-v1.0-SNAPSHOT.jar", "all", "mysql-pxc"),
		POLAR_DB_MYSQL(BASE_PATH + "connectors/dist/polar-db-mysql-connector-v1.0-SNAPSHOT.jar", "all", "polar-db-mysql"),
		POLAR_DB_POSTGRES(BASE_PATH + "connectors/dist/polar-db-postgres-connector-v1.0-SNAPSHOT.jar", "all", "polar-db-postgres"),
		TENCENT_DB_MARIADB(BASE_PATH + "connectors/dist/tencent-db-mariadb-connector-v1.0-SNAPSHOT.jar", "all", "tencent-db-mariadb"),
		TENCENT_DB_MONGODB(BASE_PATH + "connectors/dist/tencent-db-mongodb-connector-v1.0-SNAPSHOT.jar", "all", "tencent-db-mongodb"),
		TENCENT_DB_POSTGRES(BASE_PATH + "connectors/dist/tencent-db-postgres-connector-v1.0-SNAPSHOT.jar", "all", "tencent-db-postgres"),
		SelectDB(BASE_PATH + "connectors/dist/selectdb-connector-v1.0-SNAPSHOT.jar", "all", "selectdb"),
		Metabase(BASE_PATH + "connectors/dist/metabase-connector-v1.0-SNAPSHOT.jar", "all", "metabase"),
		LarkIM(BASE_PATH + "connectors/dist/lark-im-connector-v1.0-SNAPSHOT.jar", "all", "lark-im"),
		Databend(BASE_PATH + "connectors/dist/databend-connector-v1.0-SNAPSHOT.jar", "all", "databend"),
		Hazelcast(BASE_PATH + "connectors/dist/hazelcast-connector-v1.0-SNAPSHOT.jar", "all", "hazelcast"),
		ZohoCRM(BASE_PATH + "connectors/dist/zoho-crm-connector-v1.0-SNAPSHOT.jar", "all", "zoho-crm"),
		GitHubCRM(BASE_PATH + "connectors/dist/github-connector-v1.0-SNAPSHOT.jar", "all", "github"),
		LarkTask(BASE_PATH + "connectors/dist/lark-task-connector-v1.0-SNAPSHOT.jar", "all", "lark-task"),
		OpenGauss(BASE_PATH + "connectors/dist/openGauss-connector-v1.0-SNAPSHOT.jar", "all", "open-gauss", "basic", "jdbc"),
		Salesforce(BASE_PATH + "connectors/dist/salesforce-connector-v1.0-SNAPSHOT.jar", "all", "salesforce"),
		HubSpot(BASE_PATH + "connectors/dist/hubspot-connector-v1.0-SNAPSHOT.jar", "all", "hubspot"),
		Hive3(BASE_PATH + "connectors/dist/hive3-connector-v1.0-SNAPSHOT.jar", "all", "hive3"),
		BesChannels(BASE_PATH + "connectors/dist/beschannels-connector-v1.0-SNAPSHOT.jar", "all", "bes-channels"),
		LarkDoc(BASE_PATH + "connectors/dist/lark-doc-connector-v1.0-SNAPSHOT.jar", "all", "lark-doc"),
		MRSHIVE3(BASE_PATH + "connectors/dist/mrs-hive3-connector-v1.0-SNAPSHOT.jar", "all", "mrs-hive3"),
		AIChat(BASE_PATH + "connectors/dist/ai-chat-connector-v1.0-SNAPSHOT.jar", "all", "ai-chat"),
		LarkApproval(BASE_PATH + "connectors/dist/lark-approval-connector-v1.0-SNAPSHOT.jar", "all", "lark-approval"),
		HttpReceiver(BASE_PATH + "connectors/dist/http-receiver-connector-v1.0-SNAPSHOT.jar", "all", "http-receiver"),
		Shein(BASE_PATH + "connectors/dist/shein-connector-v1.0-SNAPSHOT.jar", "all", "shein"),
		YashanDB(BASE_PATH + "connectors/dist/yashandb-connector-v1.0-SNAPSHOT.jar", "all", "yashandb", "basic", "jdbc"),
		Ali1688(BASE_PATH + "connectors/dist/ali1688-connector-v1.0-SNAPSHOT.jar", "all", "ali1688"),
        Temu(BASE_PATH + "connectors/dist/temu-connector-v1.0-SNAPSHOT.jar", "all", "temu"),
        GreenPlum(BASE_PATH + "connectors/dist/greenplum-connector-v1.0-SNAPSHOT.jar", "all", "greenplum", "basic", "jdbc"),
		LarkBitable(BASE_PATH + "connectors/dist/lark-bitable-connector-v1.0-SNAPSHOT.jar", "all", "lark-bitable"),
		AzureCosmosDB(BASE_PATH + "connectors/dist/azure-cosmosdb-connector-v1.0-SNAPSHOT.jar","all","azure-cosmosdb"),
		HuDi(BASE_PATH + "connectors/dist/hudi-connector-v1.0-SNAPSHOT.jar", "all", "hudi"),
		HuaWeiOpenGaussDB(BASE_PATH + "connectors/dist/huawei-cloud-gaussdb-connector-v1.0-SNAPSHOT.jar", "all", "huawei-gauss-db"),
		Vastbase(BASE_PATH + "connectors/dist/vastbase-connector-v1.0-SNAPSHOT.jar", "all", "vastbase", "basic", "jdbc"),
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
		// -Dfilter=GA

		List<String> postList = new ArrayList<>();
		//String server = System.getProperty("server", "https://v3.test.cloud.tapdata.net/tm");
		String server = System.getProperty("server", "http://127.0.0.1:3000");
		String filter = System.getProperty("filter", "");
		//String server = System.getProperty("server", "http://192.168.1.189:30205");
		Collections.addAll(postList, "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-ak", "", "-sk", "","-f",filter, "-t", server);
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
