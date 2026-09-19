package io.tapdata.flow.engine.V2.script.storage;

import com.tapdata.entity.Connections;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private static final class RecordingStorage implements TapFileStorage {
        private final Map<String, byte[]> files = new LinkedHashMap<>();

        @Override
        public void init(Map<String, Object> params) {
        }

        @Override
        public void destroy() {
        }

        @Override
        public TapFile getFile(String path) {
            byte[] content = files.get(path);
            if (content == null) return null;
            return new TapFile().type(TapFile.TYPE_FILE).path(path).length((long) content.length).lastModified(0L);
        }

        @Override
        public void readFile(String path, Consumer<InputStream> consumer) throws Exception {
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
            return getFile(path);
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
}
