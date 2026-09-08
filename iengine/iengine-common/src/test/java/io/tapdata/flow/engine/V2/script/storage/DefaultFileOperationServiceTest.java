package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileCopyRequest;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.FileOperationResult;
import io.tapdata.file.operation.FileOperationStatus;
import io.tapdata.file.operation.FileStorageCapability;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultFileOperationServiceTest {

    @Test
    void copyUsesManagedSourceReadAndPublishesWithAtomicRename() {
        RecordingStorage source = new RecordingStorage("source");
        RecordingStorage target = new RecordingStorage("target");
        DefaultFileStorageSessionManager sessions = new DefaultFileStorageSessionManager(
                endpoint -> "source".equals(endpoint.getProtocol()) ? source : target, 4, 60_000L);
        DefaultFileOperationService service = new DefaultFileOperationService(sessions);

        FileCopyRequest request = FileCopyRequest.builder()
                .source(FileEndpoint.builder().protocol("source").build())
                .target(FileEndpoint.builder().protocol("target").build())
                .sourcePath("source.txt")
                .targetPath("target.txt")
                .overwrite(true)
                .build();

        FileOperationResult result = service.copy(request);

        assertEquals(FileOperationStatus.COPIED, result.getStatus());
        assertTrue(source.inputClosed.get());
        assertEquals("/target.txt", target.renamedTarget);
        assertTrue(target.deletedPath.startsWith("/target.txt.tapdata-tmp-"));
        assertEquals("data", new String(target.finalContent.toByteArray(), StandardCharsets.UTF_8));
        service.close();
    }

    private static final class RecordingStorage implements TapFileStorage {
        private final String protocol;
        private final AtomicBoolean inputClosed = new AtomicBoolean();
        private final ByteArrayOutputStream finalContent = new ByteArrayOutputStream();
        private String renamedTarget;
        private String deletedPath;
        private byte[] temporaryContent;

        private RecordingStorage(String protocol) {
            this.protocol = protocol;
        }

        @Override
        public void init(Map<String, Object> params) {
        }

        @Override
        public void destroy() {
        }

        @Override
        public TapFile getFile(String path) {
            if ("source".equals(protocol) && "/source.txt".equals(path)) {
                return new TapFile().type(TapFile.TYPE_FILE).path(path).name("source.txt").length(4L);
            }
            if ("target".equals(protocol) && "/target.txt".equals(path)) {
                return new TapFile().type(TapFile.TYPE_FILE).path(path).name("target.txt").length(4L);
            }
            return null;
        }

        @Override
        public void readFile(String path, java.util.function.Consumer<InputStream> consumer) {
            consumer.accept(new InputStream() {
                private final ByteArrayInputStream delegate = new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8));

                @Override
                public int read() {
                    return delegate.read();
                }

                @Override
                public int read(byte[] buffer, int offset, int length) {
                    return delegate.read(buffer, offset, length);
                }

                @Override
                public void close() {
                    inputClosed.set(true);
                }
            });
        }

        @Override
        public InputStream readFile(String path) {
            return null;
        }

        @Override
        public boolean isFileExist(String path) {
            return false;
        }

        @Override
        public boolean move(String sourcePath, String destPath) {
            renamedTarget = destPath;
            finalContent.reset();
            try {
                finalContent.write(temporaryContent);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        @Override
        public boolean delete(String path) {
            deletedPath = path;
            return true;
        }

        @Override
        public TapFile saveFile(String path, InputStream inputStream, boolean canReplace) throws IOException {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[128];
            int length;
            while ((length = inputStream.read(buffer)) >= 0) {
                if (length > 0) output.write(buffer, 0, length);
            }
            temporaryContent = output.toByteArray();
            return new TapFile().type(TapFile.TYPE_FILE).path(path).name(path).length((long) temporaryContent.length);
        }

        @Override
        public OutputStream openFileOutputStream(String path, boolean append) {
            return new ByteArrayOutputStream();
        }

        @Override
        public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                        Collection<String> excludeRegs, boolean recursive, int batchSize,
                                        java.util.function.Consumer<List<TapFile>> consumer) {
        }

        @Override
        public boolean isDirectoryExist(String path) {
            return false;
        }

        @Override
        public String getConnectInfo() {
            return protocol;
        }

        @Override
        public EnumSet<FileStorageCapability> capabilities() {
            return EnumSet.of(FileStorageCapability.ATOMIC_RENAME);
        }
    }
}
