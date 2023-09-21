package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

public class TDDCodingMain1 {
    public static void main(String... args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
                "test",
                "-c",
                "tapdata-cli/src/main/resources/config/coding.json",
                "./connectors/dist/coding-connector-v1.0-SNAPSHOT.jar",
        };
        Main.registerCommands().execute(args);
    }
}
