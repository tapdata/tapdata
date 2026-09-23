package io.tapdata.flow.engine.V2.script.storage;

import com.tapdata.entity.Connections;
import io.tapdata.file.TapFileStorage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void repeatedEventLevelAccessReusesOneExecutor() throws Throwable {
        AtomicInteger created = new AtomicInteger();
        FakeExecutor executor = new FakeExecutor("ftp-4");
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> {
                    created.incrementAndGet();
                    return executor;
                }, 0L);

        for (int i = 0; i < 100; i++) {
            assertSame(executor, manager.getStorageExecutor("ftp-4"));
        }

        assertEquals(1, created.get());
        manager.close();
        assertEquals(1, executor.closed.get());
    }

    @Test
    void closedManagerRejectsNewConnections() throws Throwable {
        FakeExecutor executor = new FakeExecutor("ftp-5");
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> executor,
                0L);

        assertSame(executor, manager.getStorageExecutor("ftp-5"));
        manager.close();

        StorageOperationException error = assertThrows(StorageOperationException.class,
                () -> manager.getStorageExecutor("ftp-5"));
        assertTrue(error.getMessage().contains("closed"));
        assertEquals(1, executor.closed.get());
    }

    @Test
    void closeDuringCreationClosesLateExecutor() throws Exception {
        CountDownLatch factoryStarted = new CountDownLatch(1);
        CountDownLatch allowFactoryToFinish = new CountDownLatch(1);
        FakeExecutor executor = new FakeExecutor("ftp-6");
        AtomicReference<Throwable> failure = new AtomicReference<>();
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> {
                    factoryStarted.countDown();
                    if (!allowFactoryToFinish.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("factory did not finish");
                    }
                    return executor;
                }, 0L);
        Thread creator = new Thread(() -> {
            try {
                manager.getStorageExecutor("ftp-6");
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        });

        creator.start();
        factoryStarted.await(5, TimeUnit.SECONDS);
        manager.close();
        allowFactoryToFinish.countDown();
        creator.join(5000L);

        assertTrue(failure.get() instanceof StorageOperationException);
        assertEquals(1, executor.closed.get());
    }

    @Test
    void invalidationDoesNotHoldManagerMonitorWhileClosingExecutor() throws Throwable {
        BlockingCloseExecutor first = new BlockingCloseExecutor("ftp-7");
        FakeExecutor second = new FakeExecutor("ftp-7");
        AtomicInteger created = new AtomicInteger();
        StorageExecutorsManager manager = new StorageExecutorsManager(
                name -> connection(name),
                (name, connections) -> created.incrementAndGet() == 1 ? first : second,
                0L);
        assertSame(first, manager.getStorageExecutor("ftp-7"));

        AtomicReference<Throwable> getterFailure = new AtomicReference<>();
        CountDownLatch getterDone = new CountDownLatch(1);
        Thread invalidator = new Thread(() -> manager.invalidate("ftp-7", new IOException("remote")));
        invalidator.start();
        assertTrue(first.closeStarted.await(5, TimeUnit.SECONDS));
        Thread getter = new Thread(() -> {
            try {
                assertSame(second, manager.getStorageExecutor("ftp-7"));
            } catch (Throwable throwable) {
                getterFailure.set(throwable);
            } finally {
                getterDone.countDown();
            }
        });
        getter.start();
        try {
            assertTrue(getterDone.await(3, TimeUnit.SECONDS));
            assertTrue(getterFailure.get() == null, () -> "executor recreation failed: " + getterFailure.get());
        } finally {
            first.allowClose.countDown();
            invalidator.join(5000);
            getter.join(5000);
            manager.close();
        }
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

    private static class FakeExecutor implements StorageExecutor {
        private final String name;
        private final AtomicInteger closed = new AtomicInteger();

        private FakeExecutor(String name) {
            this.name = name;
        }

        @Override public String getConnectionName() { return name; }
        @Override public TapFileStorage getStorage() { return null; }
        @Override public String resolvePath(String path) { return path; }
        @Override public void close() { closed.incrementAndGet(); }
    }

    private static final class BlockingCloseExecutor extends FakeExecutor {
        private final CountDownLatch closeStarted = new CountDownLatch(1);
        private final CountDownLatch allowClose = new CountDownLatch(1);

        private BlockingCloseExecutor(String name) {
            super(name);
        }

        @Override
        public void close() {
            closeStarted.countDown();
            try {
                allowClose.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.close();
        }
    }
}
