package io.tapdata.flow.engine.V2.node.duckdb;

/**
 * DuckDB SQL 节点全局配置
 * 
 * 用于控制节点行为的全局开关，支持环境变量覆盖
 */
public class DuckDbSqlConfig {

    private static final String ENV_USE_NEW_UPDATER = "DUCKDB_USE_NEW_WIDE_TABLE_UPDATER";

    /** 是否使用新的 WideTableIncrementalUpdater（默认 true） */
    private static volatile boolean useNewWideTableUpdater = readFromEnv();

    /**
     * 从环境变量读取配置
     */
    private static boolean readFromEnv() {
        String envValue = System.getenv(ENV_USE_NEW_UPDATER);
        if (envValue != null) {
            return Boolean.parseBoolean(envValue);
        }
        return true; // 默认启用新组件
    }

    /**
     * 获取是否使用新组件
     */
    public static boolean isUseNewWideTableUpdater() {
        return useNewWideTableUpdater;
    }

    /**
     * 设置是否使用新组件
     * 
     * @param value true=使用新组件，false=使用旧组件（fallback）
     */
    public static void setUseNewWideTableUpdater(boolean value) {
        useNewWideTableUpdater = value;
    }

    /**
     * 重置为环境变量默认值（用于测试）
     */
    static void resetToDefault() {
        useNewWideTableUpdater = readFromEnv();
    }
}
