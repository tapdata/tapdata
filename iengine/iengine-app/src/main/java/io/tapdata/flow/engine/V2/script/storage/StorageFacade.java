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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final boolean readOnly;

    public StorageFacade(StorageExecutorsManager executorsManager) {
        this(executorsManager, false);
    }

    public static StorageFacade readOnly(StorageExecutorsManager executorsManager) {
        return new StorageFacade(executorsManager, true);
    }

    private StorageFacade(StorageExecutorsManager executorsManager, boolean readOnly) {
        if (executorsManager == null) {
            throw new IllegalArgumentException("executorsManager is required");
        }
        this.executorsManager = executorsManager;
        this.readOnly = readOnly;
    }

    public Map<String, Object> update(String targetConnectionName,
                                      Map<String, Object> data,
                                      Map<String, Object> options) throws Throwable {
        Map<String, Object> request = requiredMap(data, "data");
        Map<String, Object> operationOptions = options == null ? Collections.emptyMap() : options;
        String action = stringValue(request, "action", "write").toLowerCase(Locale.ROOT);
        if (readOnly) {
            return readOnlyUpdate(action, request);
        }
        StorageExecutor target = executor(targetConnectionName);
        if ("copy".equals(action)) {
            return copy(target, request, operationOptions);
        }
        if ("write".equals(action)) {
            try {
                return write(target, request, operationOptions);
            } catch (Throwable throwable) {
                invalidateOnRemoteFailure(targetConnectionName, target, throwable);
                throw throwable;
            }
        }
        throw new StorageOperationException("Unsupported storage.update action: " + action);
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
            invalidateOnRemoteFailure(connectionName, executor, throwable);
            throw throwable;
        }
    }

    public boolean exists(String connectionName, String path) throws Throwable {
        StorageExecutor executor = executor(connectionName);
        try {
            return executor.getStorage().isFileExist(executor.resolvePath(requiredPath(path, "path")));
        } catch (Throwable throwable) {
            invalidateOnRemoteFailure(connectionName, executor, throwable);
            throw throwable;
        }
    }

    public boolean delete(String connectionName,
                          Map<String, Object> data,
                          Map<String, Object> options) throws Throwable {
        Map<String, Object> request = requiredMap(data, "data");
        if (readOnly) {
            validatedReadOnlyPath(request.get("path"), "path");
            return true;
        }
        StorageExecutor executor = executor(connectionName);
        try {
            return executor.getStorage().delete(executor.resolvePath(requiredPath(request, "path")));
        } catch (Throwable throwable) {
            invalidateOnRemoteFailure(connectionName, executor, throwable);
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
        StorageContentType contentType = StorageContentType.fromValue(
                request.get("contentType") == null ? null : String.valueOf(request.get("contentType")));
        validateContent(content, contentType);

        String overwriteMode = overwriteMode(options);
        return target.withStorageOperation(storage -> {
            if (!"overwrite".equals(overwriteMode)) {
                boolean exists = storage.isFileExist(path);
                if (exists && "fail".equals(overwriteMode)) {
                    throw new StorageOperationException("Target file already exists: " + path);
                }
                if (exists && "skip".equals(overwriteMode)) {
                    return result("reused", null, path);
                }
            }

            InputStream input = contentStream(content, contentType);
            if (contentType.isCallerOwned()) {
                TapFile saved = storage.saveFile(path, input, true);
                return result("written", saved, path);
            }
            try (input) {
                TapFile saved = storage.saveFile(path, input, true);
                return result("written", saved, path);
            }
        });
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
        String overwriteMode = overwriteMode(options);
        if (!"overwrite".equals(overwriteMode)) {
            try {
                Map<String, Object> earlyResult = target.withStorageOperation(storage -> {
                    boolean targetExists = storage.isFileExist(resolvedTargetPath);
                    if (!targetExists) {
                        return null;
                    }
                    if ("fail".equals(overwriteMode)) {
                        throw new StorageOperationException(
                                "Target file already exists: " + resolvedTargetPath);
                    }
                    return result("reused", storage.getFile(resolvedTargetPath), resolvedTargetPath);
                });
                if (earlyResult != null) {
                    return earlyResult;
                }
            } catch (Throwable throwable) {
                invalidateOnRemoteFailure(target.getConnectionName(), target, throwable);
                throw throwable;
            }
        }

        TapFile sourceFile;
        try {
            sourceFile = sourceExecutor.getStorage().getFile(resolvedSourcePath);
        } catch (Throwable throwable) {
            invalidateOnRemoteFailure(sourceConnectionName, sourceExecutor, throwable);
            throw throwable;
        }
        if (sourceFile == null || sourceFile.getType() == null || sourceFile.getType() != TapFile.TYPE_FILE) {
            throw new StorageOperationException("Source file does not exist: " + resolvedSourcePath);
        }

        return copyThroughTempFile(sourceExecutor, resolvedSourcePath, sourceFile,
                target, resolvedTargetPath, overwriteMode);
    }

    private Map<String, Object> copyThroughTempFile(StorageExecutor sourceExecutor,
                                                     String sourcePath,
                                                     TapFile sourceFile,
                                                     StorageExecutor targetExecutor,
                                                     String targetPath,
                                                     String overwriteMode) throws Throwable {
        TapFileStorage sourceStorage = sourceExecutor.getStorage();
        Path tempFile = Files.createTempFile("tapdata-js-storage-", UUID.randomUUID().toString());
        try {
            AtomicBoolean readCallbackInvoked = new AtomicBoolean();
            try {
                sourceStorage.readFile(sourcePath, input -> {
                    readCallbackInvoked.set(true);
                    try (InputStream sourceInput = input) {
                        Files.copy(sourceInput, tempFile, REPLACE_EXISTING);
                    } catch (Exception e) {
                        throw new StorageOperationException("Read source file failed: " + sourcePath, e);
                    }
                });
            } catch (Throwable throwable) {
                invalidateOnRemoteFailure(sourceExecutor.getConnectionName(), sourceExecutor, throwable);
                throw throwable;
            }
            if (!readCallbackInvoked.get()) {
                throw new StorageOperationException("Source file could not be read: " + sourcePath);
            }
            Long sourceLength = sourceFile.getLength();
            if (sourceLength != null && Files.size(tempFile) != sourceLength) {
                throw new StorageOperationException("Copied source file length does not match metadata: " + sourcePath);
            }
            try {
                return targetExecutor.withStorageOperation(targetStorage -> {
                    if (!"overwrite".equals(overwriteMode)) {
                        boolean targetExists = targetStorage.isFileExist(targetPath);
                        if (targetExists && "fail".equals(overwriteMode)) {
                            throw new StorageOperationException("Target file already exists: " + targetPath);
                        }
                        if (targetExists && "skip".equals(overwriteMode)) {
                            return result("reused", targetStorage.getFile(targetPath), targetPath);
                        }
                    }
                    try (InputStream targetInput = Files.newInputStream(tempFile)) {
                        TapFile saved = targetStorage.saveFile(targetPath, targetInput, true);
                        return result("copied", saved, targetPath);
                    } catch (StorageOperationException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new StorageOperationException("Copy file failed: " + sourcePath, e);
                    }
                });
            } catch (Throwable throwable) {
                invalidateOnRemoteFailure(targetExecutor.getConnectionName(), targetExecutor, throwable);
                throw throwable;
            }
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

    private String validatedReadOnlyPath(Object value, String name) {
        String path = requiredPath(value, name);
        PdkStorageExecutor.resolvePath("", path);
        return path;
    }

    private String stringValue(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value == null ? defaultValue : String.valueOf(value);
    }

    private String overwriteMode(Map<String, Object> options) {
        String mode = stringValue(options, "overwrite", "skip").toLowerCase(Locale.ROOT);
        if (!"skip".equals(mode) && !"overwrite".equals(mode) && !"fail".equals(mode)) {
            throw new StorageOperationException("overwrite must be skip, overwrite or fail");
        }
        return mode;
    }

    private InputStream contentStream(Object content, StorageContentType contentType) {
        return switch (contentType) {
            case TEXT -> textContentStream(content);
            case BYTES -> new ByteArrayInputStream(toBytes(content));
            case STREAM -> streamContentStream(content);
        };
    }

    private Map<String, Object> readOnlyUpdate(String action,
                                                Map<String, Object> request) {
        if ("write".equals(action)) {
            String targetPath = validatedReadOnlyPath(targetPath(request).get("path"), "path");
            Object content = request.get("content");
            if (content == null) {
                throw new StorageOperationException("storage.update write requires content");
            }
            StorageContentType contentType = StorageContentType.fromValue(
                    request.get("contentType") == null ? null : String.valueOf(request.get("contentType")));
            validateContent(content, contentType);
            return readOnlyResult("written", targetPath);
        }
        if ("copy".equals(action)) {
            Map<String, Object> source = requiredMap(request.get("source"), "source");
            requiredPath(source, "connection");
            validatedReadOnlyPath(source.get("path"), "path");
            String targetPath = validatedReadOnlyPath(
                    requiredMap(request.get("target"), "target").get("path"), "path");
            return readOnlyResult("copied", targetPath);
        }
        throw new StorageOperationException("Unsupported storage.update action: " + action);
    }

    private Map<String, Object> readOnlyResult(String status, String targetPath) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", status);
        result.put("targetPath", targetPath);
        return result;
    }

    private void validateContent(Object content, StorageContentType contentType) {
        switch (contentType) {
            case TEXT -> validateTextContent(content);
            case BYTES -> toBytes(content);
            case STREAM -> streamContentStream(content);
        }
    }

    private void validateTextContent(Object content) {
        if (content instanceof byte[]) {
            throw new StorageOperationException(
                    "content is byte[]; set contentType to bytes");
        }
        if (content instanceof ByteArrayOutputStream) {
            throw new StorageOperationException(
                    "content is ByteArrayOutputStream; set contentType to bytes");
        }
        if (content instanceof InputStream) {
            throw new StorageOperationException(
                    "content is InputStream; set contentType to stream");
        }
        if (content instanceof Map || content instanceof List || content.getClass().isArray()) {
            throw new StorageOperationException(
                    "content is an object or array; serialize it with JSON.stringify or set contentType explicitly");
        }
    }

    private InputStream textContentStream(Object content) {
        validateTextContent(content);
        return new ByteArrayInputStream(
                String.valueOf(content).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private InputStream streamContentStream(Object content) {
        if (!(content instanceof InputStream)) {
            throw new StorageOperationException("content must be InputStream when contentType is stream");
        }
        return (InputStream) content;
    }

    private byte[] toBytes(Object content) {
        if (content instanceof byte[]) {
            return (byte[]) content;
        }
        if (content instanceof ByteArrayOutputStream) {
            return ((ByteArrayOutputStream) content).toByteArray();
        }
        if (!(content instanceof List<?>)) {
            throw new StorageOperationException(
                    "content must be byte[] or a number list when contentType is bytes");
        }

        List<?> values = (List<?>) content;
        byte[] bytes = new byte[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (!(value instanceof Number)) {
                throw new StorageOperationException("content byte at index " + i + " must be a number");
            }
            Number number = (Number) value;
            if (number instanceof Double || number instanceof Float) {
                double decimal = number.doubleValue();
                if (!Double.isFinite(decimal) || decimal != Math.rint(decimal)) {
                    throw new StorageOperationException("content byte at index " + i + " must be an integer");
                }
            }
            long numericValue = number.longValue();
            if (numericValue < -128 || numericValue > 255) {
                throw new StorageOperationException(
                        "content byte at index " + i + " is out of range: " + numericValue);
            }
            bytes[i] = (byte) numericValue;
        }
        return bytes;
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
        if (("written".equals(status) || "copied".equals(status)) && file == null) {
            throw new StorageOperationException("File storage did not save target path: " + targetPath);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", status);
        result.put("targetPath", targetPath);
        if (file != null) {
            result.put("bytes", file.getLength());
        }
        return result;
    }

    private void invalidateOnRemoteFailure(String connectionName,
                                           StorageExecutor executor,
                                           Throwable throwable) {
        if (connectionName != null && executor != null && isRemoteFailure(throwable)) {
            executorsManager.invalidate(connectionName, executor, throwable);
        }
    }

    private boolean isRemoteFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (!(current instanceof StorageOperationException)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
