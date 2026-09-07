package com.tapdata.processor.js;

import io.tapdata.file.operation.FileBatchResult;
import io.tapdata.file.operation.FileCopyRequest;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.FileOperationException;
import io.tapdata.file.operation.FileOperationResult;
import io.tapdata.file.operation.FileVerifyMode;
import io.tapdata.file.operation.TapFileOperationService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Narrow JS facade that translates Maps to the shared file operation API. */
public final class FileScriptExecutor implements AutoCloseable {
    private final TapFileOperationService service;
    private final DefaultJsNodeConfigAccessor config;

    public FileScriptExecutor(TapFileOperationService service, DefaultJsNodeConfigAccessor config) {
        if (service == null) {
            throw new FileOperationException(io.tapdata.file.operation.FileOperationErrorCode.FILE_SERVICE_UNAVAILABLE,
                    "file operation service is unavailable");
        }
        this.service = service;
        this.config = config;
    }

    public Map<String, Object> copy(Map<?, ?> request) {
        return result(service.copy(buildRequest(stringObjectMap(request, "request"))));
    }

    public Map<String, Object> copyByConfig(Map<?, ?> request) {
        Map<String, Object> values = stringObjectMap(request, "request");
        String sourcePrefix = requiredString(values, "sourcePrefix");
        String targetPrefix = requiredString(values, "targetPrefix");
        Map<String, Object> translated = new LinkedHashMap<>(values);
        translated.put("source", endpointFromPrefix(sourcePrefix));
        translated.put("target", endpointFromPrefix(targetPrefix));
        return copy(translated);
    }

    public Map<String, Object> copyBatch(List<?> requests) {
        if (requests == null) {
            throw configError("requests must not be null");
        }
        List<FileCopyRequest> converted = new ArrayList<>();
        for (Object request : requests) {
            converted.add(buildRequest(stringObjectMap(request, "batch request")));
        }
        return batchResult(service.copyBatch(converted));
    }

    public boolean exists(Map<?, ?> request) {
        Map<String, Object> values = stringObjectMap(request, "request");
        FileEndpoint endpoint = endpoint(values.get("endpoint"));
        String path = requiredString(values, "path");
        return service.exists(endpoint, path);
    }

    @Override
    public void close() {
        service.close();
    }

    private FileCopyRequest buildRequest(Map<String, Object> values) {
        try {
            FileCopyRequest.Builder builder = FileCopyRequest.builder()
                    .source(endpoint(values.get("source")))
                    .target(endpoint(values.get("target")))
                    .sourcePath(requiredString(values, "sourcePath"))
                    .targetPath(requiredString(values, "targetPath"))
                    .overwrite(booleanValue(values, "overwrite", false))
                    .verifyMode(verifyMode(values.get("verify")))
                    .expectedChecksum(stringValue(values.get("expectedChecksum")))
                    .retryTimes(intValue(values, "retryTimes", 0))
                    .timeoutMs(longValue(values, "timeoutMs", 120_000L))
                    .dryRun(booleanValue(values, "dryRun", false));
            return builder.build();
        } catch (FileOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new FileOperationException(io.tapdata.file.operation.FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "invalid file copy request", e);
        }
    }

    private FileEndpoint endpointFromPrefix(String prefix) {
        if (config == null) throw configError("jsNodeConfig is unavailable");
        Map<String, Object> values = config.snapshotByPrefix(prefix);
        String protocol = requiredString(values, "protocol");
        String rootPath = stringValue(values.remove("rootPath"));
        if (rootPath == null) rootPath = stringValue(values.remove("path"));
        values.remove("sourcePrefix");
        values.remove("targetPrefix");
        return FileEndpoint.builder().protocol(protocol).rootPath(rootPath).params(values).build();
    }

    private FileEndpoint endpoint(Object raw) {
        if (raw instanceof FileEndpoint) return (FileEndpoint) raw;
        Map<String, Object> values = stringObjectMap(raw, "endpoint");
        String protocol = requiredString(values, "protocol");
        String rootPath = stringValue(values.get("rootPath"));
        Object params = values.get("params");
        Map<String, Object> mappedParams = params == null ? new LinkedHashMap<>() : stringObjectMap(params, "endpoint.params");
        return FileEndpoint.builder().protocol(protocol).rootPath(rootPath).params(mappedParams).build();
    }

    private static Map<String, Object> result(FileOperationResult value) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", value.getStatus().name());
        result.put("sourcePath", value.getSourcePath());
        result.put("targetPath", value.getTargetPath());
        result.put("bytes", value.getBytes());
        result.put("checksum", value.getChecksum());
        result.put("attempts", value.getAttempts());
        result.put("durationMs", value.getDurationMs());
        return result;
    }

    private static Map<String, Object> batchResult(FileBatchResult value) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", value.isSuccess());
        result.put("copied", value.getCopied());
        result.put("reused", value.getReused());
        result.put("bytes", value.getBytes());
        List<Map<String, Object>> items = new ArrayList<>();
        value.getItems().forEach(item -> items.add(result(item)));
        result.put("items", items);
        return result;
    }

    private static Map<String, Object> stringObjectMap(Object raw, String name) {
        if (!(raw instanceof Map)) throw configError(name + " must be an object");
        Map<String, Object> result = new LinkedHashMap<>();
        ((Map<?, ?>) raw).forEach((key, value) -> {
            if (!(key instanceof String)) throw configError(name + " keys must be strings");
            result.put((String) key, value);
        });
        return result;
    }

    private static String requiredString(Map<String, Object> values, String key) {
        String value = stringValue(values.get(key));
        if (value == null || value.trim().isEmpty()) throw configError(key + " is required");
        return value;
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static boolean booleanValue(Map<String, Object> values, String key, boolean fallback) {
        Object value = values.get(key);
        if (value == null) return fallback;
        if (value instanceof Boolean) return (Boolean) value;
        throw configError(key + " must be boolean");
    }

    private static int intValue(Map<String, Object> values, String key, int fallback) {
        Object value = values.get(key);
        if (value == null) return fallback;
        if (value instanceof Number) return ((Number) value).intValue();
        throw configError(key + " must be number");
    }

    private static long longValue(Map<String, Object> values, String key, long fallback) {
        Object value = values.get(key);
        if (value == null) return fallback;
        if (value instanceof Number) return ((Number) value).longValue();
        throw configError(key + " must be number");
    }

    private static FileVerifyMode verifyMode(Object value) {
        if (value == null) return FileVerifyMode.SIZE;
        try {
            return FileVerifyMode.valueOf(String.valueOf(value).toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw configError("verify must be NONE, SIZE or CHECKSUM");
        }
    }

    private static FileOperationException configError(String message) {
        return new FileOperationException(io.tapdata.file.operation.FileOperationErrorCode.FILE_CONFIG_INVALID, message);
    }
}
