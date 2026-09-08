package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.operation.FileCopyRequest;
import io.tapdata.file.operation.FileOperationErrorCode;
import io.tapdata.file.operation.FileOperationException;
import io.tapdata.file.operation.FileOperationResult;
import io.tapdata.file.operation.FileOperationStatus;
import io.tapdata.file.operation.FileVerifyMode;
import io.tapdata.file.operation.FileMetadata;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class StorageFacade {

    private final StorageExecutorsManager executorsManager;

    public StorageFacade(StorageExecutorsManager executorsManager) {
        if (executorsManager == null) {
            throw new IllegalArgumentException("executorsManager is required");
        }
        this.executorsManager = executorsManager;
    }

    public Map<String, Object> update(String targetConnectionName,
                                      Map<String, Object> data,
                                      Map<String, Object> options) throws Throwable {
        Map<String, Object> request = requiredMap(data, "data");
        Map<String, Object> operationOptions = options == null ? Collections.emptyMap() : options;
        String action = stringValue(request, "action", "write").toLowerCase();
        StorageExecutor target = executor(targetConnectionName);
        try {
            if ("copy".equals(action)) {
                return copy(targetConnectionName, target, request, operationOptions);
            }
            if ("write".equals(action)) {
                return write(target, request, operationOptions);
            }
            throw new FileOperationException(FileOperationErrorCode.FILE_UNSUPPORTED_OPERATION,
                    "Unsupported storage.update action: " + action);
        } catch (Throwable throwable) {
            invalidateIfRemote(targetConnectionName, throwable);
            throw throwable;
        }
    }

    public Map<String, Object> find(String connectionName,
                                    Map<String, Object> query,
                                    Map<String, Object> options) throws Throwable {
        Map<String, Object> request = requiredMap(query, "query");
        String path = stringValue(request, "path", null);
        StorageExecutor executor = executor(connectionName);
        try {
            FileMetadata metadata = executor.getOperationService().stat(executor.getEndpoint(), path);
            return metadata == null ? null : metadataMap(metadata);
        } catch (Throwable throwable) {
            invalidateIfRemote(connectionName, throwable);
            throw throwable;
        }
    }

    public boolean exists(String connectionName, String path) throws Throwable {
        StorageExecutor executor = executor(connectionName);
        try {
            return executor.getOperationService().exists(executor.getEndpoint(), path);
        } catch (Throwable throwable) {
            invalidateIfRemote(connectionName, throwable);
            throw throwable;
        }
    }

    public boolean delete(String connectionName,
                          Map<String, Object> data,
                          Map<String, Object> options) throws Throwable {
        Map<String, Object> request = requiredMap(data, "data");
        String path = stringValue(request, "path", null);
        StorageExecutor executor = executor(connectionName);
        try {
            return executor.getOperationService().delete(executor.getEndpoint(), path);
        } catch (Throwable throwable) {
            invalidateIfRemote(connectionName, throwable);
            throw throwable;
        }
    }

    private Map<String, Object> write(StorageExecutor target,
                                      Map<String, Object> request,
                                      Map<String, Object> options) throws Exception {
        String path = stringValue(targetPath(request), "path", null);
        Object content = request.get("content");
        if (content == null) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "storage.update write requires content");
        }
        boolean overwrite = overwrite(options);
        if ("fail".equals(overwriteMode(options))
                && target.getOperationService().exists(target.getEndpoint(), path)) {
            throw new FileOperationException(FileOperationErrorCode.FILE_TARGET_CONFLICT,
                    "Target file already exists: " + path);
        }
        byte[] bytes = content instanceof byte[]
                ? (byte[]) content
                : String.valueOf(content).getBytes(StandardCharsets.UTF_8);
        if (booleanValue(options, "dryRun", false)) {
            return resultMap(FileOperationResult.builder().status(FileOperationStatus.DRY_RUN)
                    .targetPath(path).bytes(bytes.length).attempts(1).build());
        }
        try (InputStream inputStream = new ByteArrayInputStream(bytes)) {
            FileOperationResult result = target.getOperationService()
                    .write(target.getEndpoint(), path, inputStream, overwrite);
            return resultMap(result);
        }
    }

    private Map<String, Object> copy(String targetConnectionName,
                                     StorageExecutor target,
                                     Map<String, Object> request,
                                     Map<String, Object> options) throws Throwable {
        Map<String, Object> source = requiredMap(request.get("source"), "source");
        String sourceConnectionName = stringValue(source, "connection", null);
        String sourcePath = stringValue(source, "path", null);
        Map<String, Object> targetData = requiredMap(request.get("target"), "target");
        String targetPath = stringValue(targetData, "path", null);
        StorageExecutor sourceExecutor = executor(sourceConnectionName);
        try {
            if ("fail".equals(overwriteMode(options))
                    && target.getOperationService().exists(target.getEndpoint(), targetPath)) {
                throw new FileOperationException(FileOperationErrorCode.FILE_TARGET_CONFLICT,
                        "Target file already exists: " + targetPath);
            }
            FileCopyRequest copyRequest = FileCopyRequest.builder()
                    .source(sourceExecutor.getEndpoint())
                    .target(target.getEndpoint())
                    .sourcePath(relativePath(sourcePath))
                    .targetPath(relativePath(targetPath))
                    .overwrite(overwrite(options))
                    .verifyMode(verifyMode(options))
                    .retryTimes(intValue(options, "retryTimes", 0))
                    .timeoutMs(longValue(options, "timeoutMs", 120_000L))
                    .dryRun(booleanValue(options, "dryRun", false))
                    .build();
            FileOperationResult result = target.getOperationService().copy(copyRequest);
            return resultMap(result);
        } catch (Throwable throwable) {
            invalidateIfRemote(sourceConnectionName, throwable);
            throw throwable;
        }
    }

    private StorageExecutor executor(String connectionName) throws Throwable {
        return executorsManager.getStorageExecutor(connectionName);
    }

    private Map<String, Object> targetPath(Map<String, Object> request) {
        return requiredMap(request.get("target"), "target");
    }

    private Map<String, Object> requiredMap(Object value, String name) {
        if (!(value instanceof Map)) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    name + " must be an object");
        }
        return (Map<String, Object>) value;
    }

    private String stringValue(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value == null ? defaultValue : String.valueOf(value);
    }

    private String relativePath(String path) {
        if (path == null) return null;
        String normalized = path.trim().replace('\\', '/');
        while (normalized.startsWith("/")) normalized = normalized.substring(1);
        return normalized;
    }

    private String overwriteMode(Map<String, Object> options) {
        return stringValue(options, "overwrite", "skip").toLowerCase();
    }

    private boolean overwrite(Map<String, Object> options) {
        String mode = overwriteMode(options);
        if ("overwrite".equals(mode)) return true;
        if ("skip".equals(mode) || "fail".equals(mode)) return false;
        throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                "overwrite must be skip, overwrite or fail");
    }

    private FileVerifyMode verifyMode(Map<String, Object> options) {
        String verify = stringValue(options, "verify", "size").toUpperCase();
        try {
            return FileVerifyMode.valueOf(verify);
        } catch (IllegalArgumentException e) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "verify must be none, size or checksum", e);
        }
    }

    private boolean booleanValue(Map<String, Object> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        return value == null ? defaultValue : Boolean.parseBoolean(String.valueOf(value));
    }

    private int intValue(Map<String, Object> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value == null) return defaultValue;
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    key + " must be an integer", e);
        }
    }

    private long longValue(Map<String, Object> map, String key, long defaultValue) {
        Object value = map.get(key);
        if (value == null) return defaultValue;
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    key + " must be a long", e);
        }
    }

    private Map<String, Object> metadataMap(FileMetadata metadata) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("path", metadata.getPath());
        result.put("length", metadata.getLength());
        result.put("size", metadata.getLength());
        result.put("lastModified", metadata.getLastModified());
        result.put("checksum", metadata.getChecksum());
        result.put("directory", metadata.isDirectory());
        return result;
    }

    private Map<String, Object> resultMap(FileOperationResult result) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("status", result.getStatus().name().toLowerCase());
        map.put("sourcePath", result.getSourcePath());
        map.put("targetPath", result.getTargetPath());
        map.put("bytes", result.getBytes());
        map.put("checksum", result.getChecksum());
        map.put("attempts", result.getAttempts());
        map.put("durationMs", result.getDurationMs());
        return map;
    }

    private void invalidateIfRemote(String connectionName, Throwable throwable) {
        if (connectionName == null || !isRemoteFailure(throwable)) return;
        executorsManager.invalidate(connectionName, throwable);
    }

    private boolean isRemoteFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof FileOperationException) {
                FileOperationErrorCode code = ((FileOperationException) current).getCode();
                return code == FileOperationErrorCode.FILE_CONNECT_FAILED
                        || code == FileOperationErrorCode.FILE_REMOTE_IO_FAILED
                        || code == FileOperationErrorCode.FILE_WRITE_FAILED;
            }
            current = current.getCause();
        }
        return false;
    }
}
