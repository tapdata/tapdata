package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PdkStorageExecutorTest {

    @Test
    void jsStorageRejectsMultipleReadRootsAndIgnoresNodeWritePath() {
        Map<String, Object> config = new HashMap<>();
        config.put("filePathString", "/data/in,/data/archive");
        config.put("writeFilePath", "/data/out");
        StorageOperationException error = assertThrows(StorageOperationException.class,
                () -> PdkStorageExecutor.resolveRootPath(config));
        assertTrue(error.getMessage().contains("single file root"));

        config.put("filePathString", "/data/in");
        assertEquals("/data/in", PdkStorageExecutor.resolveRootPath(config));
    }

    @Test
    void managedStorageSerializesConcurrentOperations() throws Exception {
        BlockingStorage delegate = new BlockingStorage();
        TapFileStorage managed = newManagedStorage(delegate);
        AtomicInteger secondResult = new AtomicInteger();

        Thread first = new Thread(() -> getFile(managed, "/first"));
        first.start();
        assertTrue(delegate.firstEntered.await(5, TimeUnit.SECONDS));

        Thread second = new Thread(() -> {
            getFile(managed, "/second");
            secondResult.incrementAndGet();
        });
        second.start();
        assertFalse(delegate.secondEntered.await(200, TimeUnit.MILLISECONDS));

        delegate.releaseFirst.countDown();
        first.join(5000);
        second.join(5000);
        assertEquals(1, secondResult.get());
    }

    @Test
    void managedInputStreamCanBeClosedByAnotherThread() throws Exception {
        StreamStorage delegate = new StreamStorage();
        TapFileStorage managed = newManagedStorage(delegate);
        InputStream input = managed.readFile("/stream");
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        AtomicInteger completed = new AtomicInteger();

        Thread operation = new Thread(() -> {
            getFile(managed, "/after-stream");
            completed.incrementAndGet();
        });
        operation.setDaemon(true);
        operation.start();
        assertFalse(delegate.getFileEntered.await(200, TimeUnit.MILLISECONDS));

        Thread closer = new Thread(() -> {
            try {
                input.close();
            } catch (Throwable throwable) {
                closeFailure.set(throwable);
            }
        });
        closer.start();
        closer.join(5000);
        operation.join(5000);

        assertTrue(closeFailure.get() == null, () -> "stream close failed: " + closeFailure.get());
        assertEquals(1, completed.get());
    }

    private static TapFileStorage newManagedStorage(TapFileStorage delegate) throws Exception {
        Class<?> managedClass = Class.forName(PdkStorageExecutor.class.getName() + "$PdkManagedFileStorage");
        Constructor<?> constructor = managedClass.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        return (TapFileStorage) constructor.newInstance(delegate, "test", null, null, null);
    }

    private static void getFile(TapFileStorage storage, String path) {
        try {
            storage.getFile(path);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static final class BlockingStorage implements TapFileStorage {
        private final CountDownLatch firstEntered = new CountDownLatch(1);
        private final CountDownLatch secondEntered = new CountDownLatch(1);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);
        private final AtomicInteger active = new AtomicInteger();

        @Override public void init(Map<String, Object> params) { }
        @Override public void destroy() { }

        @Override
        public TapFile getFile(String path) throws InterruptedException {
            if (active.incrementAndGet() == 1) {
                firstEntered.countDown();
                releaseFirst.await(5, TimeUnit.SECONDS);
            } else {
                secondEntered.countDown();
            }
            active.decrementAndGet();
            return new TapFile().type(TapFile.TYPE_FILE).path(path).length(0L).lastModified(0L);
        }

        @Override public void readFile(String path, Consumer<InputStream> consumer) { }
        @Override public InputStream readFile(String path) { return null; }
        @Override public boolean isFileExist(String path) { return false; }
        @Override public boolean move(String sourcePath, String destPath) { return false; }
        @Override public boolean delete(String path) { return false; }
        @Override public TapFile saveFile(String path, InputStream is, boolean canReplace) { return null; }
        @Override public OutputStream openFileOutputStream(String path, boolean append) { return null; }
        @Override public boolean supportAppendData() { return false; }
        @Override public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                                   Collection<String> excludeRegs, boolean recursive, int batchSize,
                                                   Consumer<List<TapFile>> consumer) { consumer.accept(Collections.emptyList()); }
        @Override public boolean isDirectoryExist(String path) { return false; }
        @Override public String getConnectInfo() { return "blocking"; }
    }

    private static final class StreamStorage implements TapFileStorage {
        private final CountDownLatch getFileEntered = new CountDownLatch(1);

        @Override public void init(Map<String, Object> params) { }
        @Override public void destroy() { }
        @Override public TapFile getFile(String path) {
            getFileEntered.countDown();
            return new TapFile().type(TapFile.TYPE_FILE).path(path).length(0L).lastModified(0L);
        }
        @Override public void readFile(String path, Consumer<InputStream> consumer) { }
        @Override public InputStream readFile(String path) { return new ByteArrayInputStream(new byte[0]); }
        @Override public boolean isFileExist(String path) { return false; }
        @Override public boolean move(String sourcePath, String destPath) { return false; }
        @Override public boolean delete(String path) { return false; }
        @Override public TapFile saveFile(String path, InputStream is, boolean canReplace) { return null; }
        @Override public OutputStream openFileOutputStream(String path, boolean append) { return null; }
        @Override public boolean supportAppendData() { return false; }
        @Override public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                                   Collection<String> excludeRegs, boolean recursive, int batchSize,
                                                   Consumer<List<TapFile>> consumer) { consumer.accept(Collections.emptyList()); }
        @Override public boolean isDirectoryExist(String path) { return false; }
        @Override public String getConnectInfo() { return "stream"; }
    }
}
