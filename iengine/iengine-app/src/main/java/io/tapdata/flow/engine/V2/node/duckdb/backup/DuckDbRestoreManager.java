package io.tapdata.flow.engine.V2.node.duckdb.backup;

import io.tapdata.observable.logging.ObsLogger;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class DuckDbRestoreManager {
    private final DuckDbBackupRepository repository;
    private final ObsLogger logger;

    public DuckDbRestoreManager(DuckDbBackupRepository repository, ObsLogger logger) {
        this.repository = repository;
        this.logger = logger;
    }

    public RestoreResult restoreIfNeeded(String taskId, String nodeId, String dbPath, String onCorruption) {
        if (StringUtils.isBlank(dbPath)) {
            return RestoreResult.noBackup();
        }
        Path localDb = Path.of(dbPath);
        if (Files.exists(localDb)) {
            logger.info("Local DuckDB file exists, skip HA backup restore: {}", dbPath);
            return RestoreResult.localExists();
        }
        List<Document> committed = repository.findCommitted(taskId, nodeId);
        if (committed == null || committed.isEmpty()) {
            logger.info("No committed DuckDB HA backup found, taskId={}, nodeId={}", taskId, nodeId);
            return RestoreResult.noBackup();
        }

        boolean fallbackPrevious = "FALLBACK_PREVIOUS".equalsIgnoreCase(onCorruption);
        Exception firstError = null;
        for (Document meta : committed) {
            Path archive = null;
            try {
                archive = repository.downloadArchive(meta);
                verifyArchive(meta, archive);
                DuckDbBackupFileUtil.restoreZipArchive(archive, dbPath, meta);
                logger.info("DuckDB HA backup restored, taskId={}, nodeId={}, generationId={}",
                        taskId, nodeId, meta.getString("generationId"));
                return RestoreResult.restored(meta);
            } catch (Exception e) {
                if (firstError == null) {
                    firstError = e;
                }
                logger.warn("Failed to restore DuckDB HA backup, taskId={}, nodeId={}, generationId={}, msg={}",
                        taskId, nodeId, meta.getString("generationId"), e.getMessage(), e);
                if (!fallbackPrevious) {
                    break;
                }
            } finally {
                DuckDbBackupFileUtil.deleteQuietly(archive);
            }
        }
        return RestoreResult.failed(firstError);
    }

    @SuppressWarnings("unchecked")
    private void verifyArchive(Document meta, Path archive) throws IOException {
        Object archiveObj = meta.get("archive");
        if (!(archiveObj instanceof Map<?, ?> archiveMeta)) {
            return;
        }
        Object sha = archiveMeta.get("sha256");
        if (sha == null) {
            return;
        }
        String actual = DuckDbBackupFileUtil.sha256(archive);
        if (!actual.equals(String.valueOf(sha))) {
            throw new IOException("Backup archive checksum mismatch");
        }
    }

    public static class RestoreResult {
        private final boolean restored;
        private final boolean failed;
        private final Document meta;
        private final Throwable error;

        private RestoreResult(boolean restored, boolean failed, Document meta, Throwable error) {
            this.restored = restored;
            this.failed = failed;
            this.meta = meta;
            this.error = error;
        }

        public static RestoreResult restored(Document meta) {
            return new RestoreResult(true, false, meta, null);
        }

        public static RestoreResult failed(Throwable error) {
            return new RestoreResult(false, true, null, error);
        }

        public static RestoreResult noBackup() {
            return new RestoreResult(false, false, null, null);
        }

        public static RestoreResult localExists() {
            return new RestoreResult(false, false, null, null);
        }

        public boolean isRestored() {
            return restored;
        }

        public boolean isFailed() {
            return failed;
        }

        public Document getMeta() {
            return meta;
        }

        public Throwable getError() {
            return error;
        }
    }
}
