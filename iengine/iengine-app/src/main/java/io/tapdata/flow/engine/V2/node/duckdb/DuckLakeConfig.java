package io.tapdata.flow.engine.V2.node.duckdb;

/**
 * DuckLake配置
 * 
 * 配置说明：
 * 1. enabled：是否启用DuckLake
 * 2. storageType：存储类型，支持 "LOCAL" 或 "S3"
 * 3. storagePath：存储路径
 *    - LOCAL模式：本地文件路径，例如 "/data/ducklake"
 *    - S3模式：S3路径，例如 "s3://bucket/path"
 * 4. metadataDbUrl：PostgreSQL元数据连接URL（可选）
 */
public class DuckLakeConfig {
    
    private final boolean enabled;
    private final String storageType;
    private final String storagePath;
    private final String metadataDbUrl;
    
    public DuckLakeConfig(boolean enabled, String storageType, String storagePath, String metadataDbUrl) {
        this.enabled = enabled;
        this.storageType = storageType;
        this.storagePath = storagePath;
        this.metadataDbUrl = metadataDbUrl;
    }
    
    public static DuckLakeConfig disabled() {
        return new DuckLakeConfig(false, null, null, null);
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public String getStorageType() {
        return storageType;
    }
    
    public String getStoragePath() {
        return storagePath;
    }
    
    public String getMetadataDbUrl() {
        return metadataDbUrl;
    }
    
    public boolean isLocalStorage() {
        return "LOCAL".equalsIgnoreCase(storageType);
    }
    
    public boolean isS3Storage() {
        return "S3".equalsIgnoreCase(storageType);
    }
    
    @Override
    public String toString() {
        return "DuckLakeConfig{" +
                "enabled=" + enabled +
                ", storageType='" + storageType + '\'' +
                ", storagePath='" + storagePath + '\'' +
                '}';
    }
}
