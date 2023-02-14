package io.tapdata.pdk.cli;

import io.tapdata.pdk.core.utils.CommonUtils;

public class TDDTidbMain1 {
    public static void main(String... args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
                "test",
                "-c",
                "tapdata-cli/src/main/resources/config/tidb.json",
                "./connectors/dist/tidb-connector-v1.0-SNAPSHOT.jar",
        };
        Main.registerCommands().execute(args);
    }
}

