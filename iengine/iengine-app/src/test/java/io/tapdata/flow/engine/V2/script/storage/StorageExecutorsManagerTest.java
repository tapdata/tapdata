package io.tapdata.flow.engine.V2.script.storage;

import com.tapdata.entity.Connections;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.TapFileOperationService;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

class StorageExecutorsManagerTest {

    @Test
    void concurrentFirstAccessCreatesOnlyOneExecutor() throws Exception {
        CountDownLatch factoryStarted = new CountDownLatch(1);
        CountDownLatch allowFactoryToFinish = new CountDownLatch(1);
        AtomicInteger created = new AtomicInteger();
        FakeExecutor executor = new FakeExecutor("ftp-1");
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> {
                    created.incrementAndGet();
                    factoryStarted.countDown();
                    if (!allowFactoryToFinish.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("factory did not finish");
                    }
                    return executor;
                }, 0L);
        AtomicReference<StorageExecutor> first = new AtomicReference<>();
        AtomicReference<StorageExecutor> second = new AtomicReference<>();
        Thread firstThread = new Thread(() -> first.set(get(manager, "ftp-1")));
        Thread secondThread = new Thread(() -> second.set(get(manager, "ftp-1")));

        firstThread.start();
        factoryStarted.await(5, TimeUnit.SECONDS);
        secondThread.start();
        allowFactoryToFinish.countDown();
        firstThread.join(5000);
        secondThread.join(5000);

        assertEquals(1, created.get());
        assertSame(executor, first.get());
        assertSame(executor, second.get());
        manager.close();
        assertEquals(1, executor.closed.get());
    }

    @Test
    void failedCreationUsesBackoffAndCanRecover() throws Throwable {
        AtomicInteger created = new AtomicInteger();
        FakeExecutor recovered = new FakeExecutor("ftp-2");
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> {
                    if (created.incrementAndGet() == 1) {
                        throw new IllegalStateException("temporary connection failure");
                    }
                    return recovered;
                }, 100L);

        assertThrows(IllegalStateException.class, () -> manager.getStorageExecutor("ftp-2"));
        assertThrows(IllegalStateException.class, () -> manager.getStorageExecutor("ftp-2"));
        assertEquals(1, created.get());

        Thread.sleep(150L);
        assertSame(recovered, manager.getStorageExecutor("ftp-2"));
        assertEquals(2, created.get());
        manager.close();
    }

    @Test
    void invalidationClosesCachedExecutorAndAllowsRecreation() throws Throwable {
        AtomicInteger created = new AtomicInteger();
        FakeExecutor first = new FakeExecutor("ftp-3");
        FakeExecutor second = new FakeExecutor("ftp-3");
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> created.incrementAndGet() == 1 ? first : second,
                0L);

        assertSame(first, manager.getStorageExecutor("ftp-3"));
        manager.invalidate("ftp-3", new IllegalStateException("remote failure"));
        assertEquals(1, first.closed.get());
        assertSame(second, manager.getStorageExecutor("ftp-3"));
        assertEquals(2, created.get());
        manager.close();
        assertEquals(1, second.closed.get());
    }

    private static StorageExecutor get(StorageExecutorsManager manager, String name) {
        try {
            return manager.getStorageExecutor(name);
        } catch (Throwable throwable) {
            throw new AssertionError(throwable);
        }
    }

    private static Connections connection(String name) {
        Connections connections = new Connections();
        connections.setName(name);
        return connections;
    }

    private static final class FakeExecutor implements StorageExecutor {
        private final String name;
        private final AtomicInteger closed = new AtomicInteger();

        private FakeExecutor(String name) {
            this.name = name;
        }

        @Override public String getConnectionName() { return name; }
        @Override public FileEndpoint getEndpoint() { return null; }
        @Override public TapFileOperationService getOperationService() { return null; }
        @Override public void close() { closed.incrementAndGet(); }
    }
}
