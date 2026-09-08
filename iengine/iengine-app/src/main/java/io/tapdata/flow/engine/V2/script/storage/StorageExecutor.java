package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.TapFileOperationService;

public interface StorageExecutor extends AutoCloseable {

    String getConnectionName();

    FileEndpoint getEndpoint();

    TapFileOperationService getOperationService();

    @Override
    void close();
}
