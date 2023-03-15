package io.tapdata.pdk.cli.tdd;

import io.tapdata.pdk.cli.Main;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TDDJsFactory {
    public static final String baseJarPath = "./connectors/dist/";
    public static final String baseConfPath = "tapdata-cli/src/main/resources/config/";

    private enum TddPath {
        CodingDemo("coding-demo-connector-v1.0-SNAPSHOT.jar", "js-coding.json"),
        ;
        String path;
        String conf;

        TddPath(String path, String conf) {
            this.conf = conf;
            this.path = path;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getConf() {
            return conf;
        }
    }

    public static void main(String[] args) {
        CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
        CommonUtils.setProperty("TDD_AUTO_EXIT", "0");
        args = new String[]{"test", "-c", null, null};
        for (TDDJsFactory.TddPath tddJarPath : TDDJsFactory.TddPath.values()) {
            args[2] = baseConfPath + tddJarPath.getConf();
            args[3] = baseJarPath + tddJarPath.getPath();
            Main.registerCommands().execute(args);
            try {
                TapConnectorManager instance = TapConnectorManager.getInstance();
                Class<?> cla = TapConnectorManager.class;
                Field jarNameTapConnectorMap = cla.getDeclaredField("jarNameTapConnectorMap");
                jarNameTapConnectorMap.setAccessible(true);
                jarNameTapConnectorMap.set(instance, new ConcurrentHashMap<>());
                Field externalJarManager = cla.getDeclaredField("externalJarManager");
                externalJarManager.setAccessible(true);
                externalJarManager.set(instance, null);
                Field isStarted = cla.getDeclaredField("isStarted");
                isStarted.setAccessible(true);
                isStarted.set(instance, new AtomicBoolean(false));
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        System.exit(0);
    }
}
