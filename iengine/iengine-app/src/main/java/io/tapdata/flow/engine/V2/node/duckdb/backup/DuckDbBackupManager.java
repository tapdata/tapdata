package io.tapdata.flow.engine.V2.node.duckdb.backup;

import com.tapdata.constant.UUIDGenerator;
import com.tapdata.tm.commons.dag.process.DuckDbSqlNode;
import io.tapdata.observable.logging.ObsLogger;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DuckDbBackupManager implements AutoCloseable {
    private final DuckDbSqlNode nodeConfig;
    private final String taskId;
    private final String nodeId;
    private final String nodeName;
    private final String dbPath;
    private final String dbPathFileName;
    private final DuckDbBackupRepository repository;
    private final DuckDbBackupSnapshotCallback snapshotCallback;
    private final ObsLogger logger;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile long lastBackupAt;
    private volatile long lastBackupEventSerialNo;

    public DuckDbBackupManager(DuckDbSqlNode nodeConfig,
                               String taskId,
                               String nodeId,
                               String nodeName,
                               String dbPath,
                               DuckDbBackupRepository repository,
                               DuckDbBackupSnapshotCallback snapshotCallback,
                               ObsLogger logger) {
        this.nodeConfig = nodeConfig;
        this.taskId = taskId;
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.dbPath = dbPath;
        this.dbPathFileName = Path.of(dbPath).getFileName().toString();
        this.repository = repository;
        this.snapshotCallback = snapshotCallback;
        this.logger = logger;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "duckdb-ha-backup-" + nodeId);
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        long intervalMs = valueOrDefault(nodeConfig.getHaBackupIntervalMs(), 60_000L);
        if (intervalMs > 0) {
            executor.scheduleWithFixedDelay(() -> backupNow(DuckDbBackupConstants.REASON_PERIODIC),
                    intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        }
        logger.info("DuckDB HA backup manager started, taskId={}, nodeId={}, intervalMs={}", taskId, nodeId, intervalMs);
    }

    public boolean backupNow(String reason) {
        if (closed.get() || StringUtils.isBlank(dbPath)) {
            return false;
        }
        if (!shouldBackup(reason)) {
            return false;
        }
        if (!running.compareAndSet(false, true)) {
            logger.debug("DuckDB HA backup already running, skip reason={}", reason);
            return false;
        }

        String generationId = generationId();
        Document meta = baseMeta(generationId, reason);
        DuckDbBackupSnapshot snapshot = null;
        DuckDbBackupFileUtil.ArchiveFile archive = null;
        try {
            repository.insert(meta);
            snapshot = snapshotCallback.createSnapshot(reason, generationId, meta);
            repository.updateSnapshotMeta(taskId, nodeId, generationId, meta);
            repository.markUploading(taskId, nodeId, generationId);

            archive = DuckDbBackupFileUtil.buildZipArchive(
                    snapshot.snapshotDir(), generationId, Boolean.TRUE.equals(nodeConfig.getHaBackupCompressEnabled()));
            String filename = filename(snapshot.appliedOffset(), generationId);
            Document gridFsMeta = new Document("taskId", taskId)
                    .append("nodeId", nodeId)
                    .append("generationId", generationId)
                    .append("status", DuckDbBackupConstants.STATUS_COMMITTED)
                    .append("offsetHash", offsetHash(snapshot.appliedOffset()));
            ObjectId gridFsId = repository.uploadArchive(archive.path(), filename, gridFsMeta);
            repository.markCommitted(taskId, nodeId, generationId, gridFsId, filename,
                    archive.size(), archive.sha256(), snapshot.files());

            lastBackupAt = System.currentTimeMillis();
            lastBackupEventSerialNo = eventSerialNo(snapshot.appliedOffset());
            cleanupOldBackups();
            logger.info("DuckDB HA backup committed, taskId={}, nodeId={}, generationId={}, reason={}, size={}",
                    taskId, nodeId, generationId, reason, archive.size());
            return true;
        } catch (Exception e) {
            repository.markFailed(taskId, nodeId, generationId, e);
            logger.warn("DuckDB HA backup failed, taskId={}, nodeId={}, generationId={}, reason={}, msg={}",
                    taskId, nodeId, generationId, reason, e.getMessage(), e);
            return false;
        } finally {
            if (snapshot != null) {
                DuckDbBackupFileUtil.deleteQuietly(snapshot.snapshotDir());
            }
            if (archive != null) {
                DuckDbBackupFileUtil.deleteQuietly(archive.path());
            }
            running.set(false);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            executor.shutdownNow();
        }
    }

    private boolean shouldBackup(String reason) {
        if (DuckDbBackupConstants.REASON_ENGINE_STOP.equals(reason) || DuckDbBackupConstants.REASON_FULL_COMPLETE.equals(reason)) {
            return true;
        }
        long intervalMs = valueOrDefault(nodeConfig.getHaBackupIntervalMs(), 60_000L);
        if (intervalMs <= 0) {
            return false;
        }
        if (lastBackupAt > 0 && System.currentTimeMillis() - lastBackupAt < intervalMs) {
            return false;
        }
        long minEvents = valueOrDefault(nodeConfig.getHaBackupMinEvents(), 1000L);
        if (minEvents <= 0) {
            return true;
        }
        long currentSerial = snapshotCallback.currentEventSerialNo();
        return currentSerial - lastBackupEventSerialNo >= minEvents;
    }

    private void cleanupOldBackups() {
        int retentionCount = Objects.requireNonNullElse(nodeConfig.getHaBackupRetentionCount(), 3);
        long retentionHours = Objects.requireNonNullElse(nodeConfig.getHaBackupRetentionHours(), 24L);
        repository.cleanupOldBackups(taskId, nodeId, retentionCount, retentionHours);
    }

    private Document baseMeta(String generationId, String reason) {
        long now = System.currentTimeMillis();
        return new Document("schemaVersion", 1)
                .append("taskId", taskId)
                .append("nodeId", nodeId)
                .append("nodeName", nodeName)
                .append("generationId", generationId)
                .append("status", DuckDbBackupConstants.STATUS_CREATING)
                .append("backupType", DuckDbBackupConstants.BACKUP_TYPE_FULL_FILE)
                .append("storageType", DuckDbBackupConstants.STORAGE_TYPE_GRIDFS)
                .append("dbPathFileName", dbPathFileName)
                .append("createdAt", now)
                .append("backupReason", reason)
                .append("tableGroup", "__all__");
    }

    private String filename(Map<String, Object> appliedOffset, String generationId) {
        return "duckdb-ha/tasks/" + taskId + "/nodes/" + nodeId + "/tables/__all__/offsets/"
                + offsetHash(appliedOffset) + "/" + generationId + ".zip";
    }

    private String generationId() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        format.setTimeZone(TimeZone.getDefault());
        return String.format("%012d-%s-%s",
                Math.max(0, snapshotCallback.currentEventSerialNo()),
                format.format(new Date()),
                UUIDGenerator.uuid().substring(0, 8));
    }

    private String offsetHash(Map<String, Object> appliedOffset) {
        Object offsetHash = appliedOffset == null ? null : appliedOffset.get("offsetHash");
        return offsetHash == null ? "empty" : String.valueOf(offsetHash);
    }

    private long eventSerialNo(Map<String, Object> appliedOffset) {
        Object serial = appliedOffset == null ? null : appliedOffset.get("eventSerialNo");
        if (serial instanceof Number number) {
            return number.longValue();
        }
        return snapshotCallback.currentEventSerialNo();
    }

    private long valueOrDefault(Number number, long defaultValue) {
        return number == null ? defaultValue : number.longValue();
    }

    public interface DuckDbBackupSnapshotCallback {
        DuckDbBackupSnapshot createSnapshot(String reason, String generationId, Document baseMeta) throws Exception;

        long currentEventSerialNo();
    }
}
