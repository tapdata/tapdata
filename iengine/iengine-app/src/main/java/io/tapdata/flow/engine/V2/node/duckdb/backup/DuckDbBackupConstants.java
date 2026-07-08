package io.tapdata.flow.engine.V2.node.duckdb.backup;

public final class DuckDbBackupConstants {
    public static final String COLLECTION = "DuckDbBackupFile";
    public static final String STATUS_CREATING = "CREATING";
    public static final String STATUS_UPLOADING = "UPLOADING";
    public static final String STATUS_COMMITTED = "COMMITTED";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_DELETING = "DELETING";
    public static final String REASON_PERIODIC = "PERIODIC";
    public static final String REASON_FULL_COMPLETE = "FULL_COMPLETE";
    public static final String REASON_ENGINE_STOP = "ENGINE_STOP";
    public static final String BACKUP_TYPE_FULL_FILE = "FULL_FILE";
    public static final String STORAGE_TYPE_GRIDFS = "GRIDFS";
    public static final String ARCHIVE_FORMAT_ZIP = "ZIP";

    private DuckDbBackupConstants() {
    }
}
