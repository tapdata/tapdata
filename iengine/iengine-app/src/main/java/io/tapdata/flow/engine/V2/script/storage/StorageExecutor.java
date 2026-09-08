package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFileStorage;

public interface StorageExecutor extends AutoCloseable {

    String getConnectionName();

    TapFileStorage getStorage();

    String resolvePath(String path);

    @Override
    void close();
}
