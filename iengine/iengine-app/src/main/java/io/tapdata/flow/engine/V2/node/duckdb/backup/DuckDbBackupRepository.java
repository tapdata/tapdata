package io.tapdata.flow.engine.V2.node.duckdb.backup;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DuckDbBackupRepository {
    private final ClientMongoOperator tmServerOperator;
    private final GridFSBucket gridFSBucket;

    public DuckDbBackupRepository(ClientMongoOperator tmServerOperator) {
        this.tmServerOperator = tmServerOperator;
        this.gridFSBucket = tmServerOperator.getGridFSBucket();
    }

    public void insert(Document meta) {
        tmServerOperator.insertOne(meta, DuckDbBackupConstants.COLLECTION);
    }

    public void markUploading(String taskId, String nodeId, String generationId) {
        updateStatus(taskId, nodeId, generationId, DuckDbBackupConstants.STATUS_UPLOADING, null);
    }

    public void updateSnapshotMeta(String taskId, String nodeId, String generationId, Document meta) {
        Update update = new Update();
        boolean changed = false;
        changed |= setIfPresent(update, meta, "backupReason");
        changed |= setIfPresent(update, meta, "nodeConfigHash");
        changed |= setIfPresent(update, meta, "querySqlHash");
        changed |= setIfPresent(update, meta, "schemaHash");
        changed |= setIfPresent(update, meta, "appliedOffset");
        changed |= setIfPresent(update, meta, "processState");
        changed |= setIfPresent(update, meta, "tables");
        changed |= setIfPresent(update, meta, "files");
        if (changed) {
            tmServerOperator.update(byGeneration(taskId, nodeId, generationId), update, DuckDbBackupConstants.COLLECTION);
        }
    }

    public void markCommitted(String taskId, String nodeId, String generationId, ObjectId gridFsId,
                              String filename, long archiveSize, String archiveSha256, List<Document> files) {
        Query query = byGeneration(taskId, nodeId, generationId);
        Update update = new Update()
                .set("status", DuckDbBackupConstants.STATUS_COMMITTED)
                .set("gridFsId", gridFsId.toHexString())
                .set("gridFsFilename", filename)
                .set("completedAt", System.currentTimeMillis())
                .set("archive", new Document("format", DuckDbBackupConstants.ARCHIVE_FORMAT_ZIP)
                        .append("size", archiveSize)
                        .append("sha256", archiveSha256))
                .set("files", files);
        tmServerOperator.update(query, update, DuckDbBackupConstants.COLLECTION);
    }

    public void markFailed(String taskId, String nodeId, String generationId, Throwable error) {
        updateStatus(taskId, nodeId, generationId, DuckDbBackupConstants.STATUS_FAILED,
                error == null ? null : error.getMessage());
    }

    public ObjectId uploadArchive(Path archive, String filename, Document metadata) throws IOException {
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(archive))) {
            GridFSUploadOptions options = new GridFSUploadOptions()
                    .metadata(metadata)
                    .chunkSizeBytes(1024 * 1024);
            return gridFSBucket.uploadFromStream(filename, inputStream, options);
        }
    }

    public Path downloadArchive(Document meta) throws IOException {
        String gridFsId = meta.getString("gridFsId");
        if (StringUtils.isBlank(gridFsId)) {
            throw new IOException("Backup metadata missing gridFsId");
        }
        Path archive = Files.createTempFile("tap-duckdb-restore-", ".zip");
        try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(archive))) {
            gridFSBucket.downloadToStream(new ObjectId(gridFsId), outputStream);
        }
        return archive;
    }

    public Document findLatestCommitted(String taskId, String nodeId) {
        Query query = new Query(Criteria.where("taskId").is(taskId)
                .and("nodeId").is(nodeId)
                .and("status").is(DuckDbBackupConstants.STATUS_COMMITTED));
        query.with(Sort.by(Sort.Order.desc("completedAt")));
        query.limit(1);
        return tmServerOperator.findOne(query, DuckDbBackupConstants.COLLECTION, Document.class);
    }

    public List<Document> findCommitted(String taskId, String nodeId) {
        Query query = new Query(Criteria.where("taskId").is(taskId)
                .and("nodeId").is(nodeId)
                .and("status").is(DuckDbBackupConstants.STATUS_COMMITTED));
        query.with(Sort.by(Sort.Order.desc("completedAt")));
        return tmServerOperator.find(query, DuckDbBackupConstants.COLLECTION, Document.class);
    }

    public void cleanupOldBackups(String taskId, String nodeId, int retentionCount, long retentionHours) {
        List<Document> committed = findCommitted(taskId, nodeId);
        long expireBefore = retentionHours <= 0 ? Long.MIN_VALUE : System.currentTimeMillis() - retentionHours * 3600_000L;
        List<Document> toDelete = new ArrayList<>();
        for (int i = 0; i < committed.size(); i++) {
            Document meta = committed.get(i);
            Number completedAt = meta.get("completedAt", Number.class);
            boolean overCount = retentionCount > 0 && i >= retentionCount;
            boolean expired = completedAt != null && completedAt.longValue() < expireBefore;
            if (overCount || expired) {
                toDelete.add(meta);
            }
        }
        for (Document meta : toDelete) {
            deleteBackup(meta);
        }
    }

    public void deleteBackups(String taskId, String nodeId) {
        Query query = new Query(Criteria.where("taskId").is(taskId).and("nodeId").is(nodeId));
        List<Document> all = tmServerOperator.find(query, DuckDbBackupConstants.COLLECTION, Document.class);
        for (Document meta : all) {
            deleteGridFsQuietly(meta.getString("gridFsId"));
        }
        tmServerOperator.delete(query, DuckDbBackupConstants.COLLECTION);
    }

    private void deleteBackup(Document meta) {
        String taskId = meta.getString("taskId");
        String nodeId = meta.getString("nodeId");
        String generationId = meta.getString("generationId");
        Query query = byGeneration(taskId, nodeId, generationId);
        tmServerOperator.update(query, new Update().set("status", DuckDbBackupConstants.STATUS_DELETING)
                .set("deletedAt", new Date()), DuckDbBackupConstants.COLLECTION);
        deleteGridFsQuietly(meta.getString("gridFsId"));
        tmServerOperator.delete(query, DuckDbBackupConstants.COLLECTION);
    }

    private void deleteGridFsQuietly(String gridFsId) {
        if (StringUtils.isBlank(gridFsId)) {
            return;
        }
        try {
            gridFSBucket.delete(new ObjectId(gridFsId));
        } catch (Exception ignored) {
            // GridFS cleanup is best effort; metadata cleanup should still continue.
        }
    }

    private void updateStatus(String taskId, String nodeId, String generationId, String status, String errorMessage) {
        Update update = new Update().set("status", status);
        if (errorMessage != null) {
            update.set("errorMessage", errorMessage);
        }
        tmServerOperator.update(byGeneration(taskId, nodeId, generationId), update, DuckDbBackupConstants.COLLECTION);
    }

    private boolean setIfPresent(Update update, Document meta, String key) {
        if (meta.containsKey(key)) {
            update.set(key, meta.get(key));
            return true;
        }
        return false;
    }

    private Query byGeneration(String taskId, String nodeId, String generationId) {
        return new Query(Criteria.where("taskId").is(taskId)
                .and("nodeId").is(nodeId)
                .and("generationId").is(generationId));
    }
}
