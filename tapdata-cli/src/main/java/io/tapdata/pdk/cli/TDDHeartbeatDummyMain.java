package io.tapdata.pdk.cli;

import io.tapdata.pdk.core.utils.CommonUtils;

/**
 * Dummy heartbeat test
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/11 16:29 Create
 */
public class TDDHeartbeatDummyMain {
    public static void main(String... args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        args = new String[]{
                "test", "-c", "plugin-kit/tapdata-pdk-cli/src/main/resources/config/dummy_heartbeat.json",
                "connectors/dummy-connector",
        };

        Main.registerCommands().execute(args);
    }
}
