package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.IOException;
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
    void resolvePathRejectsConfiguredRootItself() {
        for (String root : List.of("", "/", "/data")) {
            for (String path : List.of("/", "//", ".", "./", "///./")) {
                StorageOperationException error = assertThrows(StorageOperationException.class,
                        () -> PdkStorageExecutor.resolvePath(root, path));
                assertTrue(error.getMessage().contains("directory root"),
                        () -> "root: " + root + ", path: " + path);
            }
        }
    }

    @Test
    void resolvePathRejectsParentSegmentsButKeepsNestedCurrentSegments() {
        assertThrows(StorageOperationException.class,
                () -> PdkStorageExecutor.resolvePath("/data", "a/../b"));
        assertEquals("/data/a/./b", PdkStorageExecutor.resolvePath("/data", "a/./b"));
    }

    @Test
    void managedFtpStorageRejectsMissingParentBeforeSave() throws Exception {
        DirectoryAwareStorage delegate = new DirectoryAwareStorage(false);
        TapFileStorage managed = newManagedStorage(delegate, "ftp");

        StorageOperationException error = assertThrows(StorageOperationException.class,
                () -> managed.saveFile("/out/2026-10-12/a.csv",
                        new ByteArrayInputStream("today".getBytes()), true));

        assertEquals("Parent directory does not exist: /out/2026-10-12", error.getMessage());
        assertEquals(0, delegate.saveCalls.get());
    }

    @Test
    void managedFtpStorageSavesWhenParentExists() throws Exception {
        DirectoryAwareStorage delegate = new DirectoryAwareStorage(true);
        TapFileStorage managed = newManagedStorage(delegate, "ftp");

        TapFile saved = managed.saveFile("/out/2026-10-12/a.csv",
                new ByteArrayInputStream("today".getBytes()), true);

        assertEquals(TapFile.TYPE_FILE, saved.getType());
        assertEquals(1, delegate.directoryChecks.get());
        assertEquals(1, delegate.saveCalls.get());
    }

    @Test
    void managedNonFtpStorageDoesNotRequireParentDirectoryCheck() throws Exception {
        DirectoryAwareStorage delegate = new DirectoryAwareStorage(false);
        TapFileStorage managed = newManagedStorage(delegate, "s3fs");

        TapFile saved = managed.saveFile("/out/2026-10-12/a.csv",
                new ByteArrayInputStream("today".getBytes()), true);

        assertEquals(TapFile.TYPE_FILE, saved.getType());
        assertEquals(1, delegate.saveCalls.get());
        assertEquals(0, delegate.directoryChecks.get());
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
    void managedRawStreamsAreUnsupportedWithoutHoldingLock() throws Exception {
        StreamStorage delegate = new StreamStorage();
        TapFileStorage managed = newManagedStorage(delegate);
        assertThrows(UnsupportedOperationException.class, () -> managed.readFile("/stream"));
        assertThrows(UnsupportedOperationException.class,
                () -> managed.openFileOutputStream("/stream", false));

        managed.getFile("/after-stream");
        assertTrue(delegate.getFileEntered.await(5, TimeUnit.SECONDS));
    }

    @Test
    void destroyDoesNotWaitIndefinitelyForBlockedOperation() throws Exception {
        NonInterruptibleBlockingStorage delegate = new NonInterruptibleBlockingStorage();
        TapFileStorage managed = newManagedStorage(delegate);
        AtomicReference<Throwable> operationFailure = new AtomicReference<>();
        AtomicReference<Throwable> destroyFailure = new AtomicReference<>();

        Thread operation = new Thread(() -> {
            try {
                managed.getFile("/blocked");
            } catch (Throwable throwable) {
                operationFailure.set(throwable);
            }
        });
        operation.start();
        assertTrue(delegate.operationStarted.await(5, TimeUnit.SECONDS));

        Thread destroy = new Thread(() -> {
            try {
                managed.destroy();
            } catch (Throwable throwable) {
                destroyFailure.set(throwable);
            }
        });
        destroy.start();
        destroy.join(7000);
        assertFalse(destroy.isAlive());
        assertEquals(0, delegate.destroyCalls.get());

        delegate.release.countDown();
        operation.join(5000);
        assertTrue(delegate.destroyCalled.await(5, TimeUnit.SECONDS));
        assertFalse(operation.isAlive());
        assertEquals(1, delegate.destroyCalls.get());
        assertTrue(operationFailure.get() instanceof IOException);
        assertEquals(null, destroyFailure.get());
    }

    @Test
    void ftpDestroyForcesDelegateAfterTimeout() throws Exception {
        NonInterruptibleBlockingStorage delegate = new NonInterruptibleBlockingStorage();
        TapFileStorage managed = newManagedStorage(delegate, "ftp");
        AtomicReference<Throwable> operationFailure = new AtomicReference<>();

        Thread operation = new Thread(() -> {
            try {
                managed.getFile("/blocked");
            } catch (Throwable throwable) {
                operationFailure.set(throwable);
            }
        });
        operation.start();
        assertTrue(delegate.operationStarted.await(5, TimeUnit.SECONDS));

        Thread destroy = new Thread(() -> {
            try {
                managed.destroy();
            } catch (Throwable throwable) {
                throw new AssertionError(throwable);
            }
        });
        destroy.start();
        destroy.join(7000);

        assertFalse(destroy.isAlive());
        assertTrue(delegate.destroyCalled.await(1, TimeUnit.SECONDS));
        assertEquals(1, delegate.destroyCalls.get());

        delegate.release.countDown();
        operation.join(5000);
        assertFalse(operation.isAlive());
        assertTrue(operationFailure.get() instanceof IOException);
    }

    @Test
    void managedStorageRejectsOperationsAfterDestroyStarts() throws Exception {
        NonInterruptibleBlockingStorage delegate = new NonInterruptibleBlockingStorage();
        TapFileStorage managed = newManagedStorage(delegate);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();

        Thread operation = new Thread(() -> {
            try {
                managed.getFile("/blocked");
            } catch (Throwable throwable) {
                firstFailure.set(throwable);
            }
        });
        operation.start();
        assertTrue(delegate.operationStarted.await(5, TimeUnit.SECONDS));

        managed.destroy();

        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        Thread secondOperation = new Thread(() -> {
            try {
                managed.getFile("/after-close");
            } catch (Throwable throwable) {
                secondFailure.set(throwable);
            }
        });
        secondOperation.start();
        secondOperation.join(5000);

        delegate.release.countDown();
        operation.join(5000);
        assertTrue(delegate.destroyCalled.await(5, TimeUnit.SECONDS));

        assertFalse(operation.isAlive());
        assertFalse(secondOperation.isAlive());
        assertTrue(firstFailure.get() instanceof IOException);
        assertTrue(secondFailure.get() instanceof StorageOperationException);
    }

    private static TapFileStorage newManagedStorage(TapFileStorage delegate) throws Exception {
        return newManagedStorage(delegate, "test");
    }

    private static TapFileStorage newManagedStorage(TapFileStorage delegate, String protocol) throws Exception {
        Class<?> managedClass = Class.forName(PdkStorageExecutor.class.getName() + "$PdkManagedFileStorage");
        Constructor<?> constructor = managedClass.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        return (TapFileStorage) constructor.newInstance(delegate, "test", null, null, null, protocol);
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

    private static final class NonInterruptibleBlockingStorage implements TapFileStorage {
        private final CountDownLatch operationStarted = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch destroyCalled = new CountDownLatch(1);
        private final AtomicInteger destroyCalls = new AtomicInteger();

        @Override public void init(Map<String, Object> params) { }

        @Override
        public void destroy() {
            destroyCalls.incrementAndGet();
            destroyCalled.countDown();
        }

        @Override
        public TapFile getFile(String path) throws IOException {
            operationStarted.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }
            throw new IOException("released");
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
        @Override public String getConnectInfo() { return "non-interruptible"; }
    }

    private static final class DirectoryAwareStorage implements TapFileStorage {
        private final boolean parentExists;
        private final AtomicInteger directoryChecks = new AtomicInteger();
        private final AtomicInteger saveCalls = new AtomicInteger();

        private DirectoryAwareStorage(boolean parentExists) {
            this.parentExists = parentExists;
        }

        @Override public void init(Map<String, Object> params) { }
        @Override public void destroy() { }
        @Override public TapFile getFile(String path) { return null; }
        @Override public void readFile(String path, Consumer<InputStream> consumer) { }
        @Override public InputStream readFile(String path) { return null; }
        @Override public boolean isFileExist(String path) { return false; }
        @Override public boolean move(String sourcePath, String destPath) { return false; }
        @Override public boolean delete(String path) { return false; }

        @Override
        public TapFile saveFile(String path, InputStream is, boolean canReplace) {
            saveCalls.incrementAndGet();
            return new TapFile().type(TapFile.TYPE_FILE).path(path).length(5L).lastModified(0L);
        }

        @Override public OutputStream openFileOutputStream(String path, boolean append) { return null; }
        @Override public boolean supportAppendData() { return false; }
        @Override public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                                   Collection<String> excludeRegs, boolean recursive, int batchSize,
                                                   Consumer<List<TapFile>> consumer) { consumer.accept(Collections.emptyList()); }

        @Override
        public boolean isDirectoryExist(String path) {
            directoryChecks.incrementAndGet();
            return parentExists;
        }

        @Override public String getConnectInfo() { return "directory-aware"; }
    }
}
