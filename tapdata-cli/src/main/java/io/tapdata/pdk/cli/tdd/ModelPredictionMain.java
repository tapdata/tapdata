package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

public class ModelPredictionMain {
    public static void main(String[] args) {
		CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
                "modelPrediction", "-o", "./output",
//                "-i", "tapdata-api",
//                "-i", "tapdata-pdk-api",
//                "-i", "connectors/connector-core",
//                "-m", "/usr/local/Cellar/maven/3.6.2/libexec",
//                "-t", "io.tapdata.pdk.tdd.tests.target.CreateTableTest",
//                "-t", "io.tapdata.pdk.tdd.tests.basic.BasicTest",
//                "-t", "io.tapdata.pdk.tdd.tests.target.DMLTest",,
//                "-t", "io.tapdata.pdk.tdd.tests.source.BatchReadTest",
//                "-t", "io.tapdata.pdk.tdd.tests.source.StreamReadTest",
//                "B:\\code\\tapdata\\idaas-pdk\\connectors\\aerospike-connector\\target\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/doris-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/empty-connector-v1.1-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/file-connector-v1.0-SNAPSHOT.jar",

//				"connectors/dist/activemq-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/doris-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/dummy-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/elasticsearch-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/gbase8a-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/gbase8s-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/kafka-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/mongodb-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/mssql-connector-v1.0-SNAPSHOT.jar",
				"connectors/dist/mysql-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/redis-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/oceanbase-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/oracle-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/postgres-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/rabbitmq-connector-v1.0-SNAPSHOT.jar",
//				"connectors/dist/rocketmq-connector-v1.0-SNAPSHOT.jar",
				"connectors/dist/clickhouse-connector-v1.0-SNAPSHOT.jar",

//                "connectors/mysql-connector",
//                "connectors/postgres-connector",
//                "connectors/mongodb-connector",

//				"connectors/elasticsearch-connector",
//				"connectors/oceanbase-connector"

//                "connectors/oracle/oracle-connector",

//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/postgres-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/mysql-connector-v1.0-SNAPSHOT.jar"
        };

		Main.registerCommands().execute(args);
    }
}
