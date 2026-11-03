package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFileStorage;

public interface StorageExecutor extends AutoCloseable {

    @FunctionalInterface
    interface StorageOperation<T> {
        T apply(TapFileStorage storage) throws Exception;
    }

    String getConnectionName();

    TapFileStorage getStorage();

    String resolvePath(String path);

    default <T> T withStorageOperation(StorageOperation<T> operation) throws Exception {
        if (operation == null) {
            throw new IllegalArgumentException("storage operation is required");
        }
        return operation.apply(getStorage());
    }

    @Override
    void close();
}
