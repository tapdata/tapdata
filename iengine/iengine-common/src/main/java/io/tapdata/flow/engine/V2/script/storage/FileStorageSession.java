package io.tapdata.flow.engine.V2.script.storage;

import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileEndpoint;

import java.util.concurrent.atomic.AtomicBoolean;

public final class FileStorageSession {
    private final FileEndpoint endpoint;
    private final TapFileStorage storage;
    private final AtomicBoolean released = new AtomicBoolean();

    FileStorageSession(FileEndpoint endpoint, TapFileStorage storage) {
        this.endpoint = endpoint;
        this.storage = storage;
    }

    public FileEndpoint getEndpoint() {
        return endpoint;
    }

    public TapFileStorage getStorage() {
        return storage;
    }

    boolean markReleased() {
        return released.compareAndSet(false, true);
    }
}
