package io.tapdata.pdk.cli;

import picocli.CommandLine;

public class JarHijackerMain {
    public static void main(String[] args) {
        args = new String[]{
                "jar",
                "-m", "connectors/mongodb-connector",
                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/mongodb-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk-new/dist/mysql-connector-v1.0-SNAPSHOT.jar"
        };

		Main.registerCommands().execute(args);
    }
}
