package io.tapdata.flow.engine.V2.node.duckdb;

/**
 * DuckDB SQL 节点全局配置
 * 
 * 用于控制节点行为的全局开关，支持环境变量覆盖
 */
public class DuckDbSqlConfig {

    private static final String ENV_USE_NEW_UPDATER = "DUCKDB_USE_NEW_WIDE_TABLE_UPDATER";
    private static final String ENV_DB_PATH = "DUCKDB_DB_PATH";
    private static final String ENV_DUCKLAKE_ENABLED = "DUCKDB_DUCKLAKE_ENABLED";
    private static final String ENV_DUCKLAKE_STORAGE_TYPE = "DUCKDB_DUCKLAKE_STORAGE_TYPE";
    private static final String ENV_DUCKLAKE_STORAGE_PATH = "DUCKDB_DUCKLAKE_STORAGE_PATH";
    private static final String ENV_DUCKLAKE_METADATA_URL = "DUCKDB_DUCKLAKE_METADATA_URL";

    /** 是否使用新的 WideTableIncrementalUpdater（默认 true） */
    private static volatile boolean useNewWideTableUpdater = readUseNewUpdaterFromEnv();

    /** DuckDB 数据库文件路径（null=内存模式） */
    private static volatile String dbPath = readDbPathFromEnv();

    /** DuckLake 是否启用（默认 false） */
    private static volatile Boolean duckLakeEnabled = readDuckLakeEnabledFromEnv();

    /** DuckLake 存储类型（默认 LOCAL） */
    private static volatile String duckLakeStorageType = readDuckLakeStorageTypeFromEnv();

    /** DuckLake 存储路径（默认 /tmp/ducklake） */
    private static volatile String duckLakeStoragePath = readDuckLakeStoragePathFromEnv();

    /** DuckLake 元数据数据库 URL（默认 null） */
    private static volatile String duckLakeMetadataDbUrl = readDuckLakeMetadataUrlFromEnv();

    // ========== 环境变量读取方法 ==========

    private static boolean readUseNewUpdaterFromEnv() {
        String envValue = System.getenv(ENV_USE_NEW_UPDATER);
        if (envValue != null) {
            return Boolean.parseBoolean(envValue);
        }
        return true; // 默认启用新组件
    }

    private static String readDbPathFromEnv() {
        return System.getenv(ENV_DB_PATH); // 默认 null（内存模式）
    }

    private static Boolean readDuckLakeEnabledFromEnv() {
        String envValue = System.getenv(ENV_DUCKLAKE_ENABLED);
        if (envValue != null) {
            return Boolean.parseBoolean(envValue);
        }
        return false; // 默认禁用
    }

    private static String readDuckLakeStorageTypeFromEnv() {
        String envValue = System.getenv(ENV_DUCKLAKE_STORAGE_TYPE);
        return envValue != null ? envValue : "LOCAL"; // 默认 LOCAL
    }

    private static String readDuckLakeStoragePathFromEnv() {
        String envValue = System.getenv(ENV_DUCKLAKE_STORAGE_PATH);
        return envValue != null ? envValue : "/tmp/ducklake"; // 默认路径
    }

    private static String readDuckLakeMetadataUrlFromEnv() {
        return System.getenv(ENV_DUCKLAKE_METADATA_URL); // 默认 null
    }

    // ========== Getter/Setter 方法 ==========

    public static boolean isUseNewWideTableUpdater() {
        return useNewWideTableUpdater;
    }

    public static void setUseNewWideTableUpdater(boolean value) {
        useNewWideTableUpdater = value;
    }

    public static String getDbPath() {
        return dbPath;
    }

    public static void setDbPath(String path) {
        dbPath = path;
    }

    public static boolean isDuckLakeEnabled() {
        return duckLakeEnabled;
    }

    public static String getDuckLakeStorageType() {
        return duckLakeStorageType;
    }

    public static String getDuckLakeStoragePath() {
        return duckLakeStoragePath;
    }

    public static String getDuckLakeMetadataDbUrl() {
        return duckLakeMetadataDbUrl;
    }

    /**
     * 重置所有配置为环境变量默认值（用于测试）
     */
    static void resetToDefault() {
        useNewWideTableUpdater = readUseNewUpdaterFromEnv();
        dbPath = readDbPathFromEnv();
        duckLakeEnabled = readDuckLakeEnabledFromEnv();
        duckLakeStorageType = readDuckLakeStorageTypeFromEnv();
        duckLakeStoragePath = readDuckLakeStoragePathFromEnv();
        duckLakeMetadataDbUrl = readDuckLakeMetadataUrlFromEnv();
    }
}
