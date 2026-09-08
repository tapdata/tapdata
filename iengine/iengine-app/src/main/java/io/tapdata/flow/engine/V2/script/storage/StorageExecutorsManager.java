package io.tapdata.flow.engine.V2.script.storage;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.logger.Log;
import io.tapdata.file.operation.FileOperationErrorCode;
import io.tapdata.file.operation.FileOperationException;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public final class StorageExecutorsManager implements AutoCloseable {

    @FunctionalInterface
    public interface ConnectionResolver {
        Connections resolve(String connectionName) throws Exception;
    }

    @FunctionalInterface
    public interface StorageExecutorFactory {
        StorageExecutor create(String connectionName, Connections connections) throws Exception;
    }

    private static final long DEFAULT_FAILURE_BACKOFF_MS = 1000L;
    private final ConnectionResolver connectionResolver;
    private final StorageExecutorFactory executorFactory;
    private final long failureBackoffMs;
    private final ConcurrentMap<String, CompletableFuture<StorageExecutor>> executors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Failure> failures = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public StorageExecutorsManager(Log scriptLogger, ClientMongoOperator clientMongoOperator,
                                   HazelcastInstance hazelcastInstance, String taskId, String nodeId) {
        this(scriptLogger, clientMongoOperator, hazelcastInstance, taskId, nodeId, false);
    }

    public StorageExecutorsManager(Log scriptLogger, ClientMongoOperator clientMongoOperator,
                                   HazelcastInstance hazelcastInstance, String taskId, String nodeId,
                                   boolean trialRun) {
        this(name -> clientMongoOperator.findOne(new Query(where("name").is(name)),
                        ConnectorConstant.CONNECTION_COLLECTION, Connections.class),
                (name, connections) -> new PdkStorageExecutor(name, connections, clientMongoOperator,
                        hazelcastInstance, scriptLogger, taskId, nodeId), DEFAULT_FAILURE_BACKOFF_MS);
    }

    public StorageExecutorsManager(ConnectionResolver connectionResolver,
                                   StorageExecutorFactory executorFactory,
                                   long failureBackoffMs) {
        if (connectionResolver == null || executorFactory == null) {
            throw new IllegalArgumentException("connection resolver and executor factory are required");
        }
        if (failureBackoffMs < 0) {
            throw new IllegalArgumentException("failureBackoffMs must not be negative");
        }
        this.connectionResolver = connectionResolver;
        this.executorFactory = executorFactory;
        this.failureBackoffMs = failureBackoffMs;
    }

    public StorageExecutor getStorageExecutor(String connectionName) throws Throwable {
        ensureOpen(connectionName);
        if (connectionName == null || connectionName.trim().isEmpty()) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "File connection name is required");
        }
        String key = connectionName.trim();
        Failure failure = failures.get(key);
        if (failure != null && System.currentTimeMillis() - failure.failedAt < failureBackoffMs) {
            throw failure.error;
        }
        if (failure != null) failures.remove(key, failure);

        CompletableFuture<StorageExecutor> future = executors.get(key);
        if (future == null) {
            CompletableFuture<StorageExecutor> created = new CompletableFuture<>();
            future = executors.putIfAbsent(key, created);
            if (future == null) {
                future = created;
                createAsync(key, created);
            }
        }
        return await(key, future);
    }

    private void createAsync(String key, CompletableFuture<StorageExecutor> future) {
        try {
            Connections connections = connectionResolver.resolve(key);
            if (connections == null) {
                throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                        "File connection does not exist: " + key);
            }
            StorageExecutor executor = executorFactory.create(key, connections);
            if (executor == null) {
                throw new FileOperationException(FileOperationErrorCode.FILE_SERVICE_UNAVAILABLE,
                        "File storage executor is not available: " + key);
            }
            synchronized (this) {
                if (closed || executors.get(key) != future) {
                    closeQuietly(executor);
                    throw closedError(key);
                }
                future.complete(executor);
            }
        } catch (Throwable throwable) {
            Throwable error = unwrap(throwable);
            failures.put(key, new Failure(error, System.currentTimeMillis()));
            executors.remove(key, future);
            future.completeExceptionally(error);
        }
    }

    private StorageExecutor await(String key, CompletableFuture<StorageExecutor> future) throws Throwable {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FileOperationException(FileOperationErrorCode.FILE_TIMEOUT,
                    "Interrupted while creating file storage executor: " + key, e);
        } catch (ExecutionException e) {
            throw unwrap(e.getCause());
        }
    }

    public synchronized void invalidate(String connectionName, Throwable cause) {
        if (connectionName == null) return;
        String key = connectionName.trim();
        CompletableFuture<StorageExecutor> future = executors.remove(key);
        if (future == null) return;
        if (!future.isDone()) {
            future.whenComplete((executor, error) -> closeQuietly(executor));
            future.completeExceptionally(closedError(key));
            return;
        }
        if (future.isCompletedExceptionally()) return;
        try {
            closeQuietly(future.get());
        } catch (Throwable ignored) {
            // Invalidation must not hide the original file operation failure.
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        for (Map.Entry<String, CompletableFuture<StorageExecutor>> entry : executors.entrySet()) {
            CompletableFuture<StorageExecutor> future = entry.getValue();
            if (future.isDone() && !future.isCompletedExceptionally()) {
                try {
                    closeQuietly(future.get());
                } catch (Throwable ignored) {
                    // Continue closing other connection executors.
                }
            } else if (!future.isDone()) {
                future.whenComplete((executor, error) -> closeQuietly(executor));
                future.completeExceptionally(closedError(entry.getKey()));
            }
        }
        executors.clear();
        failures.clear();
    }

    private void ensureOpen(String connectionName) {
        if (closed) {
            throw closedError(connectionName == null ? "" : connectionName.trim());
        }
    }

    private FileOperationException closedError(String connectionName) {
        return new FileOperationException(FileOperationErrorCode.FILE_SERVICE_UNAVAILABLE,
                "File storage executor manager is closed: " + connectionName);
    }

    private void closeQuietly(StorageExecutor executor) {
        if (executor == null) return;
        try {
            executor.close();
        } catch (Throwable ignored) {
            // Continue closing other connection executors.
        }
    }

    private Throwable unwrap(Throwable throwable) {
        if (throwable instanceof ExecutionException && throwable.getCause() != null) {
            return unwrap(throwable.getCause());
        }
        return throwable;
    }

    private static final class Failure {
        private final Throwable error;
        private final long failedAt;

        private Failure(Throwable error, long failedAt) {
            this.error = error;
            this.failedAt = failedAt;
        }
    }
}
