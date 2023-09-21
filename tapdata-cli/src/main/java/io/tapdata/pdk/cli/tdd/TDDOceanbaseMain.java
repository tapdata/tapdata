package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;
import io.tapdata.pdk.core.utils.CommonUtils;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class TDDOceanbaseMain {
    public static void main(String[] args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
                "test", "-c", "tapdata-cli/src/main/resources/config/oceanbase.json",
                "connectors/oceanbase-connector",
        };
        Main.registerCommands().execute(args);
    }
}
