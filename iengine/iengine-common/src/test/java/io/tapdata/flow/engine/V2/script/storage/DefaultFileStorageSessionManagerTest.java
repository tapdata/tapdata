package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileEndpoint;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class DefaultFileStorageSessionManagerTest {

    @Test
    void reusesSessionAndDestroysStorageOnlyAfterRelease() {
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger destroyed = new AtomicInteger();
        DefaultFileStorageSessionManager manager = new DefaultFileStorageSessionManager(
                endpoint -> new RecordingStorage(opened.incrementAndGet(), destroyed), 4, 60_000L);
        FileEndpoint endpoint = FileEndpoint.builder()
                .protocol("ftp")
                .params(Collections.singletonMap("ftpHost", "ftp.example.com"))
                .rootPath("/root")
                .build();

        FileStorageSession first = manager.retain(endpoint);
        FileStorageSession second = manager.retain(endpoint);

        assertSame(first.getStorage(), second.getStorage());
        assertEquals(1, opened.get());

        manager.release(first);
        assertEquals(0, destroyed.get());
        manager.release(second);
        manager.close();
        assertEquals(1, destroyed.get());
    }

    @Test
    void invalidatedSessionDrainsBeforeDestroyAndNextRetainCreatesNewSession() {
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger destroyed = new AtomicInteger();
        DefaultFileStorageSessionManager manager = new DefaultFileStorageSessionManager(
                endpoint -> new RecordingStorage(opened.incrementAndGet(), destroyed), 4, 60_000L);
        FileEndpoint endpoint = FileEndpoint.builder().protocol("ftp").build();

        FileStorageSession oldSession = manager.retain(endpoint);
        manager.invalidate(oldSession);
        assertEquals(0, destroyed.get());

        FileStorageSession newSession = manager.retain(endpoint);
        assertNotSame(oldSession.getStorage(), newSession.getStorage());
        assertEquals(2, opened.get());

        manager.release(oldSession);
        assertEquals(1, destroyed.get());
        manager.release(newSession);
        manager.close();
        assertEquals(2, destroyed.get());
    }

    private static final class RecordingStorage implements TapFileStorage {
        private final int id;
        private final AtomicInteger destroyed;

        private RecordingStorage(int id, AtomicInteger destroyed) {
            this.id = id;
            this.destroyed = destroyed;
        }

        @Override
        public void init(Map<String, Object> params) {
        }

        @Override
        public void destroy() {
            destroyed.incrementAndGet();
        }

        @Override
        public TapFile getFile(String path) {
            return null;
        }

        @Override
        public void readFile(String path, Consumer<InputStream> consumer) {
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
            return false;
        }

        @Override
        public boolean delete(String path) {
            return false;
        }

        @Override
        public TapFile saveFile(String path, InputStream is, boolean canReplace) {
            return null;
        }

        @Override
        public OutputStream openFileOutputStream(String path, boolean append) {
            return null;
        }

        @Override
        public void getFilesInDirectory(String directoryPath,
                                        Collection<String> includeRegs,
                                        Collection<String> excludeRegs,
                                        boolean recursive,
                                        int batchSize,
                                        Consumer<List<TapFile>> consumer) {
        }

        @Override
        public boolean isDirectoryExist(String path) {
            return false;
        }

        @Override
        public String getConnectInfo() {
            return "test-" + id;
        }
    }
}
