package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileEndpoint;

public final class PdkFileStorageSessionManager implements AutoCloseable {

    @FunctionalInterface
    public interface StorageFactory {
        TapFileStorage create(FileEndpoint endpoint) throws Exception;
    }

    private final DefaultFileStorageSessionManager delegate;

    public PdkFileStorageSessionManager(StorageFactory storageFactory, int maxSessions, long idleTimeoutMs) {
        if (storageFactory == null) {
            throw new IllegalArgumentException("storageFactory is required");
        }
        this.delegate = new DefaultFileStorageSessionManager(storageFactory::create, maxSessions, idleTimeoutMs);
    }

    public DefaultFileStorageSessionManager delegate() {
        return delegate;
    }

    @Override
    public void close() {
        delegate.close();
    }
}
