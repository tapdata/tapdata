package io.tapdata.flow.engine.V2.script.storage;

import com.tapdata.entity.Connections;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageFacadeTest {

    @Test
    void updateWriteUsesExistingTapFileStorageApi() throws Throwable {
        RecordingStorage targetStorage = new RecordingStorage();
        StorageFacade facade = facade(targetStorage, null);

        Map<String, Object> result = facade.update("target-ftp",
                map("action", "write", "target", map("path", "/out/1.json"), "content", "hello"),
                map("overwrite", "overwrite"));

        assertEquals("written", result.get("status"));
        assertEquals("hello", new String(targetStorage.files.get("/out/1.json"), StandardCharsets.UTF_8));
    }

    @Test
    void contentTypeDefaultsToTextAndParsesCaseInsensitively() {
        assertSame(StorageContentType.TEXT, StorageContentType.fromValue(null));
        assertSame(StorageContentType.TEXT, StorageContentType.fromValue(""));
        assertSame(StorageContentType.TEXT, StorageContentType.fromValue(" TeXt "));
        assertSame(StorageContentType.BYTES, StorageContentType.fromValue("ByTeS"));
        assertSame(StorageContentType.STREAM, StorageContentType.fromValue("STREAM"));
    }

    @Test
    void updateWriteDefaultsToTextWithoutInferringBinaryContent() throws Throwable {
        RecordingStorage targetStorage = new RecordingStorage();
        StorageFacade facade = facade(targetStorage, null);

        facade.update("target-ftp",
                map("action", "write",
                        "target", map("path", "/out/default.txt"),
                        "content", List.of(65, 66)),
                map("overwrite", "overwrite"));

        assertEquals("[65, 66]",
                new String(targetStorage.files.get("/out/default.txt"), StandardCharsets.UTF_8));
    }

    @Test
    void updateWriteRestoresJsByteListWhenContentTypeIsBytes() throws Throwable {
        RecordingStorage targetStorage = new RecordingStorage();
        StorageFacade facade = facade(targetStorage, null);
        List<Integer> pngHeader = List.of(-119, 80, 78, 71, 13, 10, 26, 10);

        Map<String, Object> result = facade.update("target-ftp",
                map("action", "write",
                        "target", map("path", "/out/1.png"),
                        "contentType", "ByTeS",
                        "content", pngHeader),
                map("overwrite", "overwrite"));

        assertEquals("written", result.get("status"));
        assertArrayEquals(new byte[]{(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
                targetStorage.files.get("/out/1.png"));
    }

    @Test
    void updateWriteUsesInputStreamWhenContentTypeIsStream() throws Throwable {
        RecordingStorage targetStorage = new RecordingStorage();
        StorageFacade facade = facade(targetStorage, null);
        CloseTrackingInputStream input = new CloseTrackingInputStream(bytes("stream-content"));

        Map<String, Object> result = facade.update("target-ftp",
                map("action", "write",
                        "target", map("path", "/out/1.bin"),
                        "contentType", "sTrEaM",
                        "content", input),
                map("overwrite", "overwrite"));

        assertEquals("written", result.get("status"));
        assertEquals("stream-content",
                new String(targetStorage.files.get("/out/1.bin"), StandardCharsets.UTF_8));
        assertFalse(input.closed);
        input.close();
        assertTrue(input.closed);
    }

    @Test
    void updateWriteRejectsUnsupportedContentType() {
        StorageFacade facade = facade(new RecordingStorage(), null);

        assertThrows(StorageOperationException.class, () -> facade.update("target-ftp",
                map("action", "write",
                        "target", map("path", "/out/1.bin"),
                        "contentType", "binary",
                        "content", "data"),
                map("overwrite", "overwrite")));
    }

    @Test
    void updateCopyStreamsBetweenExistingTapFileStorageInstances() throws Throwable {
        RecordingStorage sourceStorage = new RecordingStorage();
        sourceStorage.files.put("/in/1.txt", bytes("source"));
        RecordingStorage targetStorage = new RecordingStorage();
        StorageFacade facade = facade(targetStorage, sourceStorage);

        Map<String, Object> result = facade.update("target-ftp",
                map("action", "copy",
                        "source", map("connection", "source-ftp", "path", "/in/1.txt"),
                        "target", map("path", "/out/1.txt")),
                map("overwrite", "overwrite"));

        assertEquals("copied", result.get("status"));
        assertEquals("source", new String(targetStorage.files.get("/out/1.txt"), StandardCharsets.UTF_8));
    }

    @Test
    void updateCopyWithinOneStorageDoesNotHoldReadLockWhileWriting() throws Throwable {
        RecordingStorage storage = new RecordingStorage();
        storage.files.put("/in/1.txt", bytes("source"));
        StorageFacade facade = facadeForSameStorage(storage);

        Map<String, Object> result = facade.update("target-ftp",
                map("action", "copy",
                        "source", map("connection", "source-ftp", "path", "/in/1.txt"),
                        "target", map("path", "/out/1.txt")),
                map("overwrite", "overwrite"));

        assertEquals("copied", result.get("status"));
        assertEquals("source", new String(storage.files.get("/out/1.txt"), StandardCharsets.UTF_8));
    }

    @Test
    void updateCopyRejectsDirectorySourceWithoutOverwritingTarget() {
        RecordingStorage storage = new RecordingStorage();
        storage.directories.add("/in");
        storage.files.put("/out/1.txt", bytes("existing"));
        StorageFacade facade = facadeForSameStorage(storage);

        assertThrows(StorageOperationException.class, () -> facade.update("target-ftp",
                map("action", "copy",
                        "source", map("connection", "source-ftp", "path", "/in"),
                        "target", map("path", "/out/1.txt")),
                map("overwrite", "overwrite")));

        assertEquals("existing", new String(storage.files.get("/out/1.txt"), StandardCharsets.UTF_8));
    }

    @Test
    void updateCopyRejectsWhenSourceReadDoesNotInvokeConsumer() {
        RecordingStorage sourceStorage = new RecordingStorage();
        sourceStorage.files.put("/in/1.txt", bytes("source"));
        sourceStorage.readWithoutCallback.add("/in/1.txt");
        RecordingStorage targetStorage = new RecordingStorage();
        StorageFacade facade = facade(targetStorage, sourceStorage);

        assertThrows(StorageOperationException.class, () -> facade.update("target-ftp",
                map("action", "copy",
                        "source", map("connection", "source-ftp", "path", "/in/1.txt"),
                        "target", map("path", "/out/1.txt")),
                map("overwrite", "overwrite")));

        assertFalse(targetStorage.files.containsKey("/out/1.txt"));
    }

    @Test
    void updateWriteRejectsNullSaveResult() {
        RecordingStorage targetStorage = new RecordingStorage();
        targetStorage.returnNullOnSave = true;
        StorageFacade facade = facade(targetStorage, null);

        assertThrows(StorageOperationException.class, () -> facade.update("target-ftp",
                map("action", "write", "target", map("path", "/out/1.json"), "content", "hello"),
                map("overwrite", "overwrite")));
    }

    @Test
    void wrappedRemoteCopyFailureInvalidatesCachedExecutors() throws Throwable {
        RecordingStorage sourceStorage = new RecordingStorage();
        sourceStorage.files.put("/in/1.txt", bytes("source"));
        AtomicInteger targetCreated = new AtomicInteger();
        RecordingStorage recoveredTarget = new RecordingStorage();
        StorageExecutorsManager manager = new StorageExecutorsManager(
                StorageFacadeTest::connection,
                (name, connections) -> {
                    if ("source-ftp".equals(name)) {
                        return new FakeExecutor(name, sourceStorage);
                    }
                    return new FakeExecutor(name,
                            targetCreated.getAndIncrement() == 0 ? new FailingSaveStorage() : recoveredTarget);
                },
                0L);
        StorageFacade facade = new StorageFacade(manager);

        assertThrows(StorageOperationException.class, () -> facade.update("target-ftp",
                map("action", "copy",
                        "source", map("connection", "source-ftp", "path", "/in/1.txt"),
                        "target", map("path", "/out/1.txt")),
                map("overwrite", "overwrite")));

        assertFalse(facade.exists("target-ftp", "/out/1.txt"));
        assertEquals(2, targetCreated.get());
        manager.close();
    }

    @Test
    void businessValidationFailureDoesNotInvalidateCachedExecutor() throws Throwable {
        RecordingStorage storage = new RecordingStorage();
        storage.files.put("/out/existing.txt", bytes("existing"));
        AtomicInteger created = new AtomicInteger();
        StorageExecutorsManager manager = new StorageExecutorsManager(
                StorageFacadeTest::connection,
                (name, connections) -> {
                    created.incrementAndGet();
                    return new FakeExecutor(name, storage);
                },
                0L);
        StorageFacade facade = new StorageFacade(manager);

        assertThrows(StorageOperationException.class, () -> facade.update("target-ftp",
                map("action", "write",
                        "target", map("path", "/out/existing.txt"),
                        "content", "replacement"),
                map("overwrite", "fail")));

        Map<String, Object> result = facade.update("target-ftp",
                map("action", "write",
                        "target", map("path", "/out/existing.txt"),
                        "content", "replacement"),
                map("overwrite", "overwrite"));

        assertEquals("written", result.get("status"));
        assertEquals(1, created.get());
        manager.close();
    }

    @Test
    void findExistsAndDeleteUseExistingTapFileStorageApi() throws Throwable {
        RecordingStorage storage = new RecordingStorage();
        storage.files.put("/out/1.txt", bytes("data"));
        StorageFacade facade = facade(storage, null);

        Map<String, Object> metadata = facade.find("target-ftp", map("path", "/out/1.txt"), null);

        assertEquals("/out/1.txt", metadata.get("path"));
        assertEquals(4L, metadata.get("size"));
        assertTrue(facade.exists("target-ftp", "/out/1.txt"));
        assertTrue(facade.delete("target-ftp", map("path", "/out/1.txt"), null));
        assertTrue(!storage.files.containsKey("/out/1.txt"));
    }

    @Test
    void unsupportedUpdateActionFailsInsideEngine() {
        StorageFacade facade = facade(new RecordingStorage(), null);

        assertThrows(StorageOperationException.class,
                () -> facade.update("target-ftp", map("action", "move"), null));
    }

    private static StorageFacade facade(RecordingStorage targetStorage, RecordingStorage sourceStorage) {
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> new FakeExecutor(name,
                        "source-ftp".equals(name) ? sourceStorage : targetStorage),
                0L);
        return new StorageFacade(manager);
    }

    private static StorageFacade facadeForSameStorage(RecordingStorage storage) {
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> new FakeExecutor(name, storage),
                0L);
        return new StorageFacade(manager);
    }

    private static Connections connection(String name) {
        Connections connections = new Connections();
        connections.setName(name);
        return connections;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class CloseTrackingInputStream extends ByteArrayInputStream {
        private boolean closed;

        private CloseTrackingInputStream(byte[] content) {
            super(content);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private static Map<String, Object> map(Object... values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put(String.valueOf(values[i]), values[i + 1]);
        }
        return result;
    }

    private static final class FakeExecutor implements StorageExecutor {
        private final String name;
        private final TapFileStorage storage;

        private FakeExecutor(String name, TapFileStorage storage) {
            this.name = name;
            this.storage = storage;
        }

        @Override
        public String getConnectionName() {
            return name;
        }

        @Override
        public TapFileStorage getStorage() {
            return storage;
        }

        @Override
        public String resolvePath(String path) {
            return path;
        }

        @Override
        public void close() {
        }
    }

    private static final class LockingStorage extends RecordingStorage {
        private final String name;
        private final CyclicBarrier readsStarted;
        private final ReentrantLock ioLock = new ReentrantLock();

        private LockingStorage(String name, CyclicBarrier readsStarted) {
            this.name = name;
            this.readsStarted = readsStarted;
        }

        @Override
        public TapFile getFile(String path) {
            ioLock.lock();
            try {
                return super.getFile(path);
            } finally {
                ioLock.unlock();
            }
        }

        @Override
        public boolean isFileExist(String path) {
            ioLock.lock();
            try {
                return super.isFileExist(path);
            } finally {
                ioLock.unlock();
            }
        }

        @Override
        public void readFile(String path, Consumer<InputStream> consumer) throws Exception {
            ioLock.lock();
            try {
                readsStarted.await(3, TimeUnit.SECONDS);
                super.readFile(path, consumer);
            } finally {
                ioLock.unlock();
            }
        }

        @Override
        public TapFile saveFile(String path, InputStream inputStream, boolean canReplace) throws Exception {
            if (!ioLock.tryLock(500, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("cross-storage lock inversion in " + name);
            }
            try {
                return super.saveFile(path, inputStream, canReplace);
            } finally {
                ioLock.unlock();
            }
        }
    }

    private static class RecordingStorage implements TapFileStorage {
        protected final Map<String, byte[]> files = new LinkedHashMap<>();
        protected final java.util.Set<String> directories = new java.util.HashSet<>();
        protected final java.util.Set<String> readWithoutCallback = new java.util.HashSet<>();
        protected boolean returnNullOnSave;

        @Override
        public void init(Map<String, Object> params) {
        }

        @Override
        public void destroy() {
        }

        @Override
        public TapFile getFile(String path) {
            if (directories.contains(path)) {
                return new TapFile().type(TapFile.TYPE_DIRECTORY).path(path).length(0L).lastModified(0L);
            }
            byte[] content = files.get(path);
            if (content == null) return null;
            return new TapFile().type(TapFile.TYPE_FILE).path(path).length((long) content.length).lastModified(0L);
        }

        @Override
        public void readFile(String path, Consumer<InputStream> consumer) throws Exception {
            if (readWithoutCallback.contains(path)) return;
            byte[] content = files.get(path);
            if (content != null) consumer.accept(new ByteArrayInputStream(content));
        }

        @Override
        public InputStream readFile(String path) {
            byte[] content = files.get(path);
            return content == null ? null : new ByteArrayInputStream(content);
        }

        @Override
        public boolean isFileExist(String path) {
            return files.containsKey(path);
        }

        @Override
        public boolean move(String sourcePath, String destPath) {
            byte[] content = files.remove(sourcePath);
            if (content == null) return false;
            files.put(destPath, content);
            return true;
        }

        @Override
        public boolean delete(String path) {
            return files.remove(path) != null;
        }

        @Override
        public TapFile saveFile(String path, InputStream inputStream, boolean canReplace) throws Exception {
            if (files.containsKey(path) && !canReplace) return getFile(path);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[128];
            int length;
            while ((length = inputStream.read(buffer)) >= 0) {
                if (length > 0) output.write(buffer, 0, length);
            }
            files.put(path, output.toByteArray());
            return returnNullOnSave ? null : getFile(path);
        }

        @Override
        public OutputStream openFileOutputStream(String path, boolean append) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                        Collection<String> excludeRegs, boolean recursive, int batchSize,
                                        Consumer<List<TapFile>> consumer) {
            consumer.accept(Collections.emptyList());
        }

        @Override
        public boolean isDirectoryExist(String path) {
            return false;
        }

        @Override
        public String getConnectInfo() {
            return "recording";
        }
    }

    private static final class FailingSaveStorage extends RecordingStorage {
        @Override
        public TapFile saveFile(String path, InputStream inputStream, boolean canReplace) throws IOException {
            throw new IOException("remote write failed");
        }
    }
}
