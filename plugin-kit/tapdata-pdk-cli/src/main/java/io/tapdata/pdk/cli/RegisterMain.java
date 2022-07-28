package io.tapdata.pdk.cli;

import java.net.URL;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class RegisterMain {
	//
	public static void main(String... args) {
		URL resource = RegisterMain.class.getClassLoader().getResource("");
		String basePath = "/";
		if (null != resource) basePath = resource.getPath() + "../../../../";
		System.out.println("basePath:"+basePath);
		args = new String[]{
				"register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-t", "http://localhost:3000",
//                "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-t", "http://192.168.1.132:31787",
//				"register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-t", "http://192.168.1.132:31966",
//				"register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-t", "http://192.168.1.181:31321",

//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/empty-connector-v1.1-SNAPSHOT.jar",
				basePath + "connectors/dist/mysql-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/postgres-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/mongodb-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/elasticsearch-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/oceanbase-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/doris-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/activemq-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/rabbitmq-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/rocketmq-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/kafka-connector-v1.0-SNAPSHOT.jar",
				basePath + "connectors/dist/clickhouse-connector-v1.0-SNAPSHOT.jar",
//				basePath + "connectors/dist/mysql-connector-v1.0-SNAPSHOT.jar",
//				"D:\\workspace\\dev\\daasv2.0\\dassv2.7-2\\tapdata\\connectors\\clickhouse-connector\\target\\clickhouse-connector-1.0-SNAPSHOT.jar"
		};

		Main.registerCommands().execute(args);
	}
}
