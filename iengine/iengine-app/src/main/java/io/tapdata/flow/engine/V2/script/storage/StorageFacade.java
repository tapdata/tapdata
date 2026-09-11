package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * JavaScript-facing file operations.
 *
 * <p>The facade intentionally delegates to the existing {@link TapFileStorage}
 * contract. It is an adapter for the script object shape, not a second file
 * connector API.</p>
 */
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
                return copy(target, request, operationOptions);
            }
            if ("write".equals(action)) {
                return write(target, request, operationOptions);
            }
            throw new StorageOperationException("Unsupported storage.update action: " + action);
        } catch (Throwable throwable) {
            invalidateOnRemoteFailure(targetConnectionName, throwable);
            throw throwable;
        }
    }

    public Map<String, Object> find(String connectionName,
                                    Map<String, Object> query,
                                    Map<String, Object> options) throws Throwable {
        Map<String, Object> request = requiredMap(query, "query");
        StorageExecutor executor = executor(connectionName);
        try {
            TapFile file = executor.getStorage().getFile(executor.resolvePath(requiredPath(request, "path")));
            return file == null ? null : metadataMap(file);
        } catch (Throwable throwable) {
            invalidateOnRemoteFailure(connectionName, throwable);
            throw throwable;
        }
    }

    public boolean exists(String connectionName, String path) throws Throwable {
        StorageExecutor executor = executor(connectionName);
        try {
            return executor.getStorage().isFileExist(executor.resolvePath(requiredPath(path, "path")));
        } catch (Throwable throwable) {
            invalidateOnRemoteFailure(connectionName, throwable);
            throw throwable;
        }
    }

    public boolean delete(String connectionName,
                          Map<String, Object> data,
                          Map<String, Object> options) throws Throwable {
        Map<String, Object> request = requiredMap(data, "data");
        StorageExecutor executor = executor(connectionName);
        try {
            return executor.getStorage().delete(executor.resolvePath(requiredPath(request, "path")));
        } catch (Throwable throwable) {
            invalidateOnRemoteFailure(connectionName, throwable);
            throw throwable;
        }
    }

    private Map<String, Object> write(StorageExecutor target,
                                      Map<String, Object> request,
                                      Map<String, Object> options) throws Exception {
        String path = target.resolvePath(requiredPath(targetPath(request), "path"));
        Object content = request.get("content");
        if (content == null) {
            throw new StorageOperationException("storage.update write requires content");
        }

        TapFileStorage storage = target.getStorage();
        String overwriteMode = overwriteMode(options);
        boolean exists = storage.isFileExist(path);
        if (exists && "fail".equals(overwriteMode)) {
            throw new StorageOperationException("Target file already exists: " + path);
        }
        if (exists && "skip".equals(overwriteMode)) {
            return result("reused", null, path);
        }

        try (InputStream input = contentStream(content)) {
            TapFile saved = storage.saveFile(path, input, true);
            return result("written", saved, path);
        }
    }

    private Map<String, Object> copy(StorageExecutor target,
                                     Map<String, Object> request,
                                     Map<String, Object> options) throws Throwable {
        Map<String, Object> source = requiredMap(request.get("source"), "source");
        String sourceConnectionName = requiredPath(source, "connection");
        String sourcePath = requiredPath(source, "path");
        Map<String, Object> targetData = requiredMap(request.get("target"), "target");
        String targetPath = requiredPath(targetData, "path");

        StorageExecutor sourceExecutor = executor(sourceConnectionName);
        String resolvedSourcePath = sourceExecutor.resolvePath(sourcePath);
        String resolvedTargetPath = target.resolvePath(targetPath);
        try {
            TapFileStorage sourceStorage = sourceExecutor.getStorage();
            TapFileStorage targetStorage = target.getStorage();
            String overwriteMode = overwriteMode(options);
            boolean targetExists = targetStorage.isFileExist(resolvedTargetPath);
            if (targetExists && "fail".equals(overwriteMode)) {
                throw new StorageOperationException("Target file already exists: " + resolvedTargetPath);
            }
            if (targetExists && "skip".equals(overwriteMode)) {
                return result("reused", targetStorage.getFile(resolvedTargetPath), resolvedTargetPath);
            }
            if (sourceStorage.getFile(resolvedSourcePath) == null) {
                throw new StorageOperationException("Source file does not exist: " + resolvedSourcePath);
            }

            if (sourceStorage == targetStorage) {
                return copyThroughTempFile(sourceStorage, resolvedSourcePath, resolvedTargetPath);
            }

            final TapFile[] saved = new TapFile[1];
            sourceStorage.readFile(resolvedSourcePath, input -> {
                try (InputStream sourceInput = input) {
                    saved[0] = targetStorage.saveFile(resolvedTargetPath, sourceInput, true);
                } catch (Exception e) {
                    throw new StorageOperationException("Copy file failed: " + resolvedSourcePath, e);
                }
            });
            return result("copied", saved[0], resolvedTargetPath);
        } catch (Throwable throwable) {
            invalidateOnRemoteFailure(sourceConnectionName, throwable);
            if (!sourceConnectionName.equals(target.getConnectionName())) {
                invalidateOnRemoteFailure(target.getConnectionName(), throwable);
            }
            throw throwable;
        }
    }

    private Map<String, Object> copyThroughTempFile(TapFileStorage storage,
                                                     String sourcePath,
                                                     String targetPath) throws Exception {
        Path tempFile = Files.createTempFile("tapdata-js-storage-", UUID.randomUUID().toString());
        try {
            storage.readFile(sourcePath, input -> {
                try (InputStream sourceInput = input) {
                    Files.copy(sourceInput, tempFile, REPLACE_EXISTING);
                } catch (Exception e) {
                    throw new StorageOperationException("Read source file failed: " + sourcePath, e);
                }
            });
            TapFile saved;
            try (InputStream targetInput = Files.newInputStream(tempFile)) {
                saved = storage.saveFile(targetPath, targetInput, true);
            }
            return result("copied", saved, targetPath);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private StorageExecutor executor(String connectionName) throws Throwable {
        return executorsManager.getStorageExecutor(connectionName);
    }

    private Map<String, Object> targetPath(Map<String, Object> request) {
        return requiredMap(request.get("target"), "target");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> requiredMap(Object value, String name) {
        if (!(value instanceof Map)) {
            throw new StorageOperationException(name + " must be an object");
        }
        return (Map<String, Object>) value;
    }

    private String requiredPath(Map<String, Object> map, String key) {
        return requiredPath(map.get(key), key);
    }

    private String requiredPath(Object value, String name) {
        if (value == null || String.valueOf(value).trim().isEmpty()) {
            throw new StorageOperationException(name + " is required");
        }
        return String.valueOf(value);
    }

    private String stringValue(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value == null ? defaultValue : String.valueOf(value);
    }

    private String overwriteMode(Map<String, Object> options) {
        String mode = stringValue(options, "overwrite", "skip").toLowerCase();
        if (!"skip".equals(mode) && !"overwrite".equals(mode) && !"fail".equals(mode)) {
            throw new StorageOperationException("overwrite must be skip, overwrite or fail");
        }
        return mode;
    }

    private InputStream contentStream(Object content) {
        if (content instanceof InputStream) {
            return (InputStream) content;
        }
        if (content instanceof byte[]) {
            return new ByteArrayInputStream((byte[]) content);
        }
        if (content instanceof ByteArrayOutputStream) {
            return new ByteArrayInputStream(((ByteArrayOutputStream) content).toByteArray());
        }
        return new ByteArrayInputStream(String.valueOf(content).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private Map<String, Object> metadataMap(TapFile file) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("path", file.getPath());
        result.put("length", file.getLength());
        result.put("size", file.getLength());
        result.put("lastModified", file.getLastModified());
        result.put("directory", file.getType() != null && file.getType() == TapFile.TYPE_DIRECTORY);
        return result;
    }

    private Map<String, Object> result(String status, TapFile file, String targetPath) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", status);
        result.put("targetPath", targetPath);
        if (file != null) {
            result.put("bytes", file.getLength());
        }
        return result;
    }

    private void invalidateOnRemoteFailure(String connectionName, Throwable throwable) {
        if (connectionName != null && isRemoteFailure(throwable)) {
            executorsManager.invalidate(connectionName, throwable);
        }
    }

    private boolean isRemoteFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof StorageOperationException) {
                return false;
            }
            current = current.getCause();
        }
        return true;
    }
}
