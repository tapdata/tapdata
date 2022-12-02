package io.tapdata.pdk.cli;

import io.tapdata.pdk.core.utils.CommonUtils;

public class TDDMssqlMain1 {
    public static void main(String[] args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
                "capabilities",
                "-o",
                "./output",
                "./connectors/dist"
        };
        Main.registerCommands().execute(args);
    }
}
