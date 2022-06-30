package io.tapdata.pdk.cli;

import picocli.CommandLine;

public class ModelPredictionMain {
    public static void main(String[] args) {
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

                "connectors/mysql-connector",
                "connectors/postgres-connector",
                "connectors/mongodb-connector",
//                "connectors/oracle/oracle-connector",

//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/postgres-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/mysql-connector-v1.0-SNAPSHOT.jar"
        };

		Main.registerCommands().execute(args);
    }
}
