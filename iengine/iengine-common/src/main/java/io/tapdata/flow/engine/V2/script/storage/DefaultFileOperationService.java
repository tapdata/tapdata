package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileAccess;
import io.tapdata.file.operation.FileBatchResult;
import io.tapdata.file.operation.FileCopyRequest;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.FileListRequest;
import io.tapdata.file.operation.FileMetadata;
import io.tapdata.file.operation.FileOperationErrorCode;
import io.tapdata.file.operation.FileOperationException;
import io.tapdata.file.operation.FileOperationResult;
import io.tapdata.file.operation.FileOperationStatus;
import io.tapdata.file.operation.FileStorageCapability;
import io.tapdata.file.operation.FileValidationResult;
import io.tapdata.file.operation.FileVerifyMode;
import io.tapdata.file.operation.TapFileOperationService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

public final class DefaultFileOperationService implements TapFileOperationService {

    private final DefaultFileStorageSessionManager sessions;
    private final int maxBatchFiles;

    public DefaultFileOperationService(DefaultFileStorageSessionManager sessions) {
        this(sessions, 100);
    }

    public DefaultFileOperationService(DefaultFileStorageSessionManager sessions, int maxBatchFiles) {
        if (sessions == null) {
            throw new IllegalArgumentException("sessions is required");
        }
        if (maxBatchFiles <= 0) {
            throw new IllegalArgumentException("maxBatchFiles must be positive");
        }
        this.sessions = sessions;
        this.maxBatchFiles = maxBatchFiles;
    }

    @Override
    public FileOperationResult copy(FileCopyRequest request) {
        if (request == null) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "File copy request is required");
        }
        FileOperationException lastFailure = null;
        int maxAttempts = request.getRetryTimes() + 1;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                FileOperationResult result = copyOnce(request, attempt);
                return result;
            } catch (FileOperationException e) {
                lastFailure = e;
                if (attempt >= maxAttempts || !isRetryable(e)) {
                    throw e;
                }
            }
        }
        throw lastFailure;
    }

    private FileOperationResult copyOnce(FileCopyRequest request, int attempt) {
        FileStorageSession sourceSession = null;
        FileStorageSession targetSession = null;
        Path localTemp = null;
        String temporaryTargetPath = null;
        String sourcePath = resolvePath(request.getSource(), request.getSourcePath());
        String targetPath = resolvePath(request.getTarget(), request.getTargetPath());
        long startedAt = System.currentTimeMillis();
        try {
            sourceSession = sessions.retain(request.getSource());
            targetSession = sessions.retain(request.getTarget());
            TapFileStorage source = sourceSession.getStorage();
            TapFileStorage target = targetSession.getStorage();
            TapFile sourceFile = source.getFile(sourcePath);
            if (sourceFile == null || sourceFile.getType() == null || sourceFile.getType() != TapFile.TYPE_FILE) {
                throw new FileOperationException(FileOperationErrorCode.FILE_NOT_FOUND,
                        "Source file does not exist: " + request.getSourcePath());
            }
            if (target.isFileExist(targetPath) && !request.isOverwrite()) {
                return result(FileOperationStatus.REUSED, request, sourceFile.getLength() == null ? 0 : sourceFile.getLength(),
                        attempt, startedAt);
            }
            if (request.isDryRun()) {
                return result(FileOperationStatus.DRY_RUN, request, sourceFile.getLength() == null ? 0 : sourceFile.getLength(),
                        attempt, startedAt);
            }
            if (request.getVerifyMode() == FileVerifyMode.CHECKSUM
                    && !target.capabilities().contains(FileStorageCapability.CHECKSUM)) {
                throw new FileOperationException(FileOperationErrorCode.FILE_UNSUPPORTED_OPERATION,
                        "Checksum verification is not supported by target storage");
            }

            localTemp = Files.createTempFile("tapdata-file-copy-", ".part");
            Path finalLocalTemp = localTemp;
            source.readFile(sourcePath, inputStream -> copyToLocalFile(inputStream, finalLocalTemp));
            long bytes = Files.size(localTemp);
            if (request.getVerifyMode() == FileVerifyMode.SIZE
                    && sourceFile.getLength() != null && sourceFile.getLength() != bytes) {
                throw new FileOperationException(FileOperationErrorCode.FILE_VERIFY_FAILED,
                        "Source file size changed during copy: " + request.getSourcePath());
            }

            temporaryTargetPath = targetPath + ".tapdata-tmp-" + UUID.randomUUID();
            try (InputStream targetInput = Files.newInputStream(localTemp, StandardOpenOption.READ)) {
                target.saveFile(temporaryTargetPath, targetInput, true);
            }
            EnumSet<FileStorageCapability> capabilities = target.capabilities();
            if (capabilities.contains(FileStorageCapability.ATOMIC_RENAME)) {
                if (!target.move(temporaryTargetPath, targetPath)) {
                    throw new FileOperationException(FileOperationErrorCode.FILE_WRITE_FAILED,
                            "Publish temporary file failed: " + request.getTargetPath());
                }
            } else {
                try (InputStream targetInput = Files.newInputStream(localTemp, StandardOpenOption.READ)) {
                    target.saveFile(targetPath, targetInput, true);
                }
                target.delete(temporaryTargetPath);
            }
            TapFile finalFile = target.getFile(targetPath);
            if (finalFile == null) {
                throw new FileOperationException(FileOperationErrorCode.FILE_VERIFY_FAILED,
                        "Published target file cannot be stat-ed: " + request.getTargetPath());
            }
            if (request.getVerifyMode() == FileVerifyMode.SIZE
                    && finalFile.getLength() != null && finalFile.getLength() != bytes) {
                throw new FileOperationException(FileOperationErrorCode.FILE_VERIFY_FAILED,
                        "Target file size does not match source: " + request.getTargetPath());
            }
            return result(FileOperationStatus.COPIED, request, bytes, attempt, startedAt);
        } catch (FileOperationException e) {
            if (isRetryable(e)) {
                invalidate(sourceSession, targetSession);
            }
            throw e;
        } catch (Exception e) {
            invalidate(sourceSession, targetSession);
            throw new FileOperationException(FileOperationErrorCode.FILE_REMOTE_IO_FAILED,
                    "Copy file failed", e);
        } finally {
            deleteRemoteTemp(targetSession, temporaryTargetPath);
            deleteLocalTemp(localTemp);
            sessions.release(sourceSession);
            sessions.release(targetSession);
        }
    }

    private void copyToLocalFile(InputStream inputStream, Path localFile) {
        try (InputStream source = inputStream;
             OutputStream output = Files.newOutputStream(localFile, StandardOpenOption.TRUNCATE_EXISTING)) {
            byte[] buffer = new byte[8192];
            int length;
            while ((length = source.read(buffer)) >= 0) {
                if (length > 0) {
                    output.write(buffer, 0, length);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public FileBatchResult copyBatch(List<FileCopyRequest> requests) {
        if (requests == null || requests.size() > maxBatchFiles) {
            throw new FileOperationException(FileOperationErrorCode.FILE_BATCH_LIMIT,
                    "File copy batch exceeds the configured limit");
        }
        List<FileOperationResult> items = new ArrayList<>();
        int copied = 0;
        int reused = 0;
        long bytes = 0;
        boolean success = true;
        for (FileCopyRequest request : requests) {
            try {
                FileOperationResult result = copy(request);
                items.add(result);
                if (result.getStatus() == FileOperationStatus.COPIED) copied++;
                if (result.getStatus() == FileOperationStatus.REUSED) reused++;
                bytes += result.getBytes();
            } catch (FileOperationException e) {
                success = false;
                items.add(FileOperationResult.builder()
                        .status(FileOperationStatus.FAILED)
                        .sourcePath(request == null ? null : request.getSourcePath())
                        .targetPath(request == null ? null : request.getTargetPath())
                        .build());
            }
        }
        return FileBatchResult.builder()
                .success(success)
                .items(items)
                .copied(copied)
                .reused(reused)
                .bytes(bytes)
                .build();
    }

    @Override
    public FileMetadata stat(FileEndpoint endpoint, String path) {
        FileStorageSession session = null;
        try {
            session = sessions.retain(endpoint);
            return metadata(sessionsStorage(session).getFile(resolvePath(endpoint, path)));
        } catch (FileOperationException e) {
            throw e;
        } catch (Exception e) {
            if (!(e instanceof FileOperationException) || isRetryable((FileOperationException) e)) {
                invalidate(session);
            }
            throw remoteFailure("Stat file failed", e);
        } finally {
            sessions.release(session);
        }
    }

    @Override
    public boolean exists(FileEndpoint endpoint, String path) {
        FileStorageSession session = null;
        try {
            session = sessions.retain(endpoint);
            return sessionsStorage(session).isFileExist(resolvePath(endpoint, path));
        } catch (Exception e) {
            if (!(e instanceof FileOperationException) || isRetryable((FileOperationException) e)) {
                invalidate(session);
            }
            throw remoteFailure("Check file existence failed", e);
        } finally {
            sessions.release(session);
        }
    }

    @Override
    public List<FileMetadata> list(FileListRequest request) {
        if (request == null || request.getEndpoint() == null) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "File list request is required");
        }
        FileStorageSession session = null;
        try {
            session = sessions.retain(request.getEndpoint());
            List<FileMetadata> result = new ArrayList<>();
            TapFileStorage storage = sessionsStorage(session);
            storage.getFilesInDirectory(resolvePath(request.getEndpoint(), request.getDirectoryPath()),
                    request.getIncludeRegs(), request.getExcludeRegs(), request.isRecursive(),
                    request.getBatchSize(), files -> addMetadata(result, files));
            return result;
        } catch (Exception e) {
            if (!(e instanceof FileOperationException) || isRetryable((FileOperationException) e)) {
                invalidate(session);
            }
            throw remoteFailure("List files failed", e);
        } finally {
            sessions.release(session);
        }
    }

    @Override
    public FileValidationResult validate(FileEndpoint endpoint, String path, FileAccess access) {
        if (endpoint == null || access == null) {
            return FileValidationResult.invalid("endpoint and access are required");
        }
        try {
            FileStorageSession session = sessions.retain(endpoint);
            try {
                if (access == FileAccess.READ && !sessionsStorage(session).isFileExist(resolvePath(endpoint, path))) {
                    return FileValidationResult.invalid("file does not exist");
                }
                return FileValidationResult.valid();
            } finally {
                sessions.release(session);
            }
        } catch (Exception e) {
            return FileValidationResult.invalid(e.getMessage());
        }
    }

    @Override
    public FileOperationResult write(FileEndpoint endpoint, String path, InputStream inputStream, boolean overwrite) {
        if (inputStream == null) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "Input stream is required");
        }
        FileStorageSession session = null;
        long startedAt = System.currentTimeMillis();
        try {
            session = sessions.retain(endpoint);
            String resolvedPath = resolvePath(endpoint, path);
            TapFileStorage storage = sessionsStorage(session);
            if (storage.isFileExist(resolvedPath) && !overwrite) {
                return FileOperationResult.builder().status(FileOperationStatus.REUSED)
                        .targetPath(path).durationMs(System.currentTimeMillis() - startedAt).build();
            }
            TapFile file;
            try (InputStream content = inputStream) {
                file = storage.saveFile(resolvedPath, content, overwrite);
            }
            return FileOperationResult.builder().status(FileOperationStatus.COPIED)
                    .targetPath(path).bytes(file == null || file.getLength() == null ? 0 : file.getLength())
                    .attempts(1).durationMs(System.currentTimeMillis() - startedAt).build();
        } catch (Exception e) {
            if (!(e instanceof FileOperationException) || isRetryable((FileOperationException) e)) {
                invalidate(session);
            }
            throw remoteFailure("Write file failed", e);
        } finally {
            sessions.release(session);
        }
    }

    @Override
    public boolean delete(FileEndpoint endpoint, String path) {
        FileStorageSession session = null;
        try {
            session = sessions.retain(endpoint);
            return sessionsStorage(session).delete(resolvePath(endpoint, path));
        } catch (Exception e) {
            if (!(e instanceof FileOperationException) || isRetryable((FileOperationException) e)) {
                invalidate(session);
            }
            throw remoteFailure("Delete file failed", e);
        } finally {
            sessions.release(session);
        }
    }

    @Override
    public void close() {
        sessions.close();
    }

    private TapFileStorage sessionsStorage(FileStorageSession session) {
        if (session == null || session.getStorage() == null) {
            throw new FileOperationException(FileOperationErrorCode.FILE_SERVICE_UNAVAILABLE,
                    "File storage session is unavailable");
        }
        return session.getStorage();
    }

    private void invalidate(FileStorageSession... sessionsToInvalidate) {
        for (FileStorageSession session : sessionsToInvalidate) {
            if (session != null) {
                sessions.invalidate(session);
            }
        }
    }

    private boolean isRetryable(FileOperationException exception) {
        return exception.getCode() == FileOperationErrorCode.FILE_REMOTE_IO_FAILED
                || exception.getCode() == FileOperationErrorCode.FILE_CONNECT_FAILED
                || exception.getCode() == FileOperationErrorCode.FILE_WRITE_FAILED;
    }

    private FileOperationException remoteFailure(String message, Exception cause) {
        if (cause instanceof FileOperationException) {
            return (FileOperationException) cause;
        }
        return new FileOperationException(FileOperationErrorCode.FILE_REMOTE_IO_FAILED, message, cause);
    }

    private FileOperationResult result(FileOperationStatus status, FileCopyRequest request, long bytes,
                                       int attempt, long startedAt) {
        return FileOperationResult.builder().status(status)
                .sourcePath(request.getSourcePath()).targetPath(request.getTargetPath())
                .bytes(bytes).attempts(attempt)
                .durationMs(System.currentTimeMillis() - startedAt).build();
    }

    private FileMetadata metadata(TapFile file) {
        if (file == null) {
            return null;
        }
        return new FileMetadata(file.getPath(), file.getLength() == null ? 0 : file.getLength(),
                file.getLastModified() == null ? 0 : file.getLastModified(), null,
                file.getType() != null && file.getType() == TapFile.TYPE_DIRECTORY);
    }

    private void addMetadata(List<FileMetadata> result, Collection<TapFile> files) {
        if (files == null) return;
        for (TapFile file : files) {
            result.add(metadata(file));
        }
    }

    private String resolvePath(FileEndpoint endpoint, String path) {
        if (endpoint == null || path == null || path.trim().isEmpty()) {
            throw new FileOperationException(FileOperationErrorCode.FILE_PATH_FORBIDDEN,
                    "File path is required");
        }
        String value = path.trim().replace('\\', '/');
        while (value.startsWith("/")) value = value.substring(1);
        if (value.contains("://") || containsParentSegment(value) || containsControlCharacter(value)) {
            throw new FileOperationException(FileOperationErrorCode.FILE_PATH_FORBIDDEN,
                    "File path is not allowed: " + path);
        }
        String root = endpoint.getRootPath() == null ? "" : endpoint.getRootPath().trim();
        if (root.isEmpty() || "/".equals(root)) {
            return "/" + value;
        }
        return (root.endsWith("/") ? root : root + "/") + value;
    }

    private boolean containsParentSegment(String path) {
        for (String segment : path.split("/")) {
            if ("..".equals(segment)) return true;
        }
        return false;
    }

    private boolean containsControlCharacter(String path) {
        for (int i = 0; i < path.length(); i++) {
            if (Character.isISOControl(path.charAt(i))) return true;
        }
        return false;
    }

    private void deleteLocalTemp(Path localTemp) {
        if (localTemp != null) {
            try {
                Files.deleteIfExists(localTemp);
            } catch (IOException ignored) {
                // Keep the primary operation result; cleanup is observable by the caller's filesystem monitor.
            }
        }
    }

    private void deleteRemoteTemp(FileStorageSession session, String path) {
        if (session == null || path == null) {
            return;
        }
        try {
            session.getStorage().delete(path);
        } catch (Throwable ignored) {
            // Preserve the primary operation result; cleanup failure is handled by storage monitoring.
        }
    }
}
