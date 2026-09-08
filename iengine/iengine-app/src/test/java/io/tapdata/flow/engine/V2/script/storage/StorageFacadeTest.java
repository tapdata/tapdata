package io.tapdata.flow.engine.V2.script.storage;

import com.tapdata.entity.Connections;
import io.tapdata.file.operation.FileAccess;
import io.tapdata.file.operation.FileBatchResult;
import io.tapdata.file.operation.FileCopyRequest;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.FileListRequest;
import io.tapdata.file.operation.FileMetadata;
import io.tapdata.file.operation.FileOperationResult;
import io.tapdata.file.operation.FileOperationStatus;
import io.tapdata.file.operation.FileValidationResult;
import io.tapdata.file.operation.FileVerifyMode;
import io.tapdata.file.operation.TapFileOperationService;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageFacadeTest {

    @Test
    void updateWriteOnlyPassesJsonContentAndReturnsPlainMap() throws Throwable {
        RecordingService service = new RecordingService();
        StorageFacade facade = facade(service);

        Map<String, Object> result = facade.update("target-ftp",
                map("action", "write", "target", map("path", "/out/1.json"), "content", "hello"),
                map("overwrite", "overwrite"));

        assertEquals("copied", result.get("status"));
        assertEquals("/out/1.json", service.writtenPath.get());
        assertEquals("hello", service.writtenContent.toString());
        assertTrue(service.writtenOverwrite);
    }

    @Test
    void updateCopyUsesDifferentSourceConnectionAndNormalizesPaths() throws Throwable {
        RecordingService targetService = new RecordingService();
        StorageFacade facade = facade(targetService);

        Map<String, Object> result = facade.update("target-ftp",
                map("action", "copy",
                        "source", map("connection", "source-ftp", "path", "/in/1.txt"),
                        "target", map("path", "/out/1.txt")),
                map("overwrite", "skip", "verify", "size", "retryTimes", 2));

        assertEquals("copied", result.get("status"));
        assertEquals("in/1.txt", targetService.copyRequest.get().getSourcePath());
        assertEquals("out/1.txt", targetService.copyRequest.get().getTargetPath());
        assertEquals(FileVerifyMode.SIZE, targetService.copyRequest.get().getVerifyMode());
        assertFalse(targetService.copyRequest.get().isOverwrite());
        assertEquals(2, targetService.copyRequest.get().getRetryTimes());
    }

    private static StorageFacade facade(RecordingService targetService) {
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> new FakeExecutor(name, "source-ftp".equals(name) ? new RecordingService() : targetService),
                0L);
        return new StorageFacade(manager);
    }

    private static Connections connection(String name) {
        Connections connections = new Connections();
        connections.setName(name);
        return connections;
    }

    private static Map<String, Object> map(Object... values) {
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) result.put(String.valueOf(values[i]), values[i + 1]);
        return result;
    }

    private static final class FakeExecutor implements StorageExecutor {
        private final String name;
        private final FileEndpoint endpoint = FileEndpoint.builder().protocol("ftp").build();
        private final TapFileOperationService service;

        private FakeExecutor(String name, TapFileOperationService service) {
            this.name = name;
            this.service = service;
        }

        @Override public String getConnectionName() { return name; }
        @Override public FileEndpoint getEndpoint() { return endpoint; }
        @Override public TapFileOperationService getOperationService() { return service; }
        @Override public void close() { }
    }

    private static final class RecordingService implements TapFileOperationService {
        private final AtomicReference<String> writtenPath = new AtomicReference<>();
        private final ByteArrayOutputStream writtenContent = new ByteArrayOutputStream();
        private final AtomicReference<FileCopyRequest> copyRequest = new AtomicReference<>();
        private boolean writtenOverwrite;

        @Override public FileOperationResult copy(FileCopyRequest request) {
            copyRequest.set(request);
            return FileOperationResult.builder().status(FileOperationStatus.COPIED)
                    .sourcePath(request.getSourcePath()).targetPath(request.getTargetPath()).bytes(4).build();
        }
        @Override public FileBatchResult copyBatch(List<FileCopyRequest> requests) { return null; }
        @Override public FileMetadata stat(FileEndpoint endpoint, String path) { return null; }
        @Override public boolean exists(FileEndpoint endpoint, String path) { return false; }
        @Override public List<FileMetadata> list(FileListRequest request) { return Collections.emptyList(); }
        @Override public FileValidationResult validate(FileEndpoint endpoint, String path, FileAccess access) { return FileValidationResult.valid(); }
        @Override public FileOperationResult write(FileEndpoint endpoint, String path, InputStream inputStream, boolean overwrite) {
            writtenPath.set(path);
            writtenOverwrite = overwrite;
            writtenContent.reset();
            byte[] buffer = new byte[128];
            int length;
            try {
                while ((length = inputStream.read(buffer)) >= 0) {
                    if (length > 0) writtenContent.write(buffer, 0, length);
                }
            } catch (java.io.IOException e) {
                throw new AssertionError(e);
            }
            return FileOperationResult.builder().status(FileOperationStatus.COPIED)
                    .targetPath(path).bytes(writtenContent.size()).build();
        }
        @Override public boolean delete(FileEndpoint endpoint, String path) { return true; }
        @Override public void close() { }
    }
}
