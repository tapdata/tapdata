package io.tapdata.pdk.cli;

import io.tapdata.pdk.core.utils.CommonUtils;

import java.sql.SQLException;

public class TDDTDengineMain {

    public static void main(String... args) throws ClassNotFoundException, SQLException {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
                "test", "-c", "plugin-kit/tapdata-pdk-cli/src/main/resources/config/tdengine.json",
                "-t", "io.tapdata.pdk.tdd.tests.basic.BasicTest",
                "-t", "io.tapdata.pdk.tdd.tests.source.StreamReadTest",
                "-t", "io.tapdata.pdk.tdd.tests.source.BatchReadTest",
                "-t", "io.tapdata.pdk.tdd.tests.target.CreateTableTest",
                "-t", "io.tapdata.pdk.tdd.tests.target.benchmark.ReadWriteBenchmarkTest",
                "-t", "io.tapdata.pdk.tdd.tests.target.DMLTest",
                "connectors/tdengine-connector",};

        Main.registerCommands().execute(args);
    }
}
