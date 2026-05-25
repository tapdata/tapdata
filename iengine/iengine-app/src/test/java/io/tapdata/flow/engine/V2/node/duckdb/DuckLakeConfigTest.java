package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DuckLakeConfigTest {

    @Test
    void testDisabledConfig() {
        DuckLakeConfig config = DuckLakeConfig.disabled();
        assertFalse(config.isEnabled());
        assertNull(config.getStorageType());
        assertNull(config.getStoragePath());
        assertNull(config.getMetadataDbUrl());
    }

    @Test
    void testLocalStorageConfig() {
        DuckLakeConfig config = new DuckLakeConfig(true, "LOCAL", "/data/ducklake", "jdbc:postgresql://localhost:5432/metadata");
        assertTrue(config.isEnabled());
        assertTrue(config.isLocalStorage());
        assertFalse(config.isS3Storage());
        assertEquals("/data/ducklake", config.getStoragePath());
        assertEquals("jdbc:postgresql://localhost:5432/metadata", config.getMetadataDbUrl());
    }

    @Test
    void testS3StorageConfig() {
        DuckLakeConfig config = new DuckLakeConfig(true, "S3", "s3://bucket/path", null);
        assertTrue(config.isEnabled());
        assertTrue(config.isS3Storage());
        assertFalse(config.isLocalStorage());
        assertEquals("s3://bucket/path", config.getStoragePath());
        assertNull(config.getMetadataDbUrl());
    }

    @Test
    void testStorageTypeCaseInsensitivity() {
        DuckLakeConfig localConfig = new DuckLakeConfig(true, "local", "/data", null);
        assertTrue(localConfig.isLocalStorage());
        
        DuckLakeConfig s3Config = new DuckLakeConfig(true, "s3", "s3://bucket", null);
        assertTrue(s3Config.isS3Storage());
    }

    @Test
    void testUnknownStorageType() {
        DuckLakeConfig config = new DuckLakeConfig(true, "UNKNOWN", "/data", null);
        assertFalse(config.isLocalStorage());
        assertFalse(config.isS3Storage());
    }

    @Test
    void testNullStorageType() {
        DuckLakeConfig config = new DuckLakeConfig(true, null, "/data", null);
        assertFalse(config.isLocalStorage());
        assertFalse(config.isS3Storage());
    }

    @Test
    void testToString() {
        DuckLakeConfig config = new DuckLakeConfig(true, "LOCAL", "/data", "jdbc:postgresql");
        String str = config.toString();
        assertTrue(str.contains("enabled=true"));
        assertTrue(str.contains("storageType='LOCAL'"));
        assertTrue(str.contains("storagePath='/data'"));
    }
}
