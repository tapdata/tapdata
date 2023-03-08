package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

/**
 * @author samuel
 * @Description
 * @create 2022-04-27 19:37
 **/
public class TDDHive1Main {
	public static void main(String... args) {
		CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
		args = new String[]{
//                "test", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\aerospike.json",
//                "test", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\doris.json",
//                "test", "-c", "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/tapdata-pdk-cli/src/main/resources/config/doris.json",
//				"test", "-c", "tapdata-cli/src/main/resources/config/clickhouse.json",
				"test", "-c", "tapdata-cli/src/main/resources/config/hive1.json",
//                "-t", "io.tapdata.pdk.tdd.tests.target.CreateTableTest",
                "-t", "io.tapdata.pdk.tdd.tests.basic.BasicTest",
//                "-t", "io.tapdata.pdk.tdd.tests.target.DMLTest",
//                "-t", "io.tapdata.pdk.tdd.tests.source.BatchReadTest",
//                "-t", "io.tapdata.pdk.tdd.tests.source.StreamReadTest",
//                "B:\\code\\tapdata\\idaas-pdk\\connectors\\aerospike-connector\\target\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/doris-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/empty-connector-v1.1-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/file-connector-v1.0-SNAPSHOT.jar",
//				"-i", "tapdata-api",
//                "-i", "tapdata-pdk-api",
//                "-i", "connectors/connector-core",
//                "-i", "connectors/mysql/mysql-core",
//				"-m", "/Users/samuel/apache-maven-3.6.1",
//				"connectors/clickhouse-connector",
//				"connectors/hive1-connector",
				"D:\\workspace\\dev\\daasv2.0\\daasv2.8\\tapdata\\connectors\\dist\\hive1-connector-v1.0-SNAPSHOT.jar",
//				"D:\\workspace\\dev\\daasv2.0\\daasv2.8\\tapdata\\connectors\\dist\\clickhouse-connector-v1.0-SNAPSHOT.jar",
//				"D:\\workspace\\dev\\daasv2.0\\daasv2.7\\tapdata\\connectors\\clickhouse-connector\\target\\clickhouse-connector-v1.0-SNAPSHOT.jar",
		};

		Main.registerCommands().execute(args);
	}
}
