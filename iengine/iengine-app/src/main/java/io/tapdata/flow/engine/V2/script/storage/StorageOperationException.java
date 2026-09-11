package io.tapdata.flow.engine.V2.script.storage;

/**
 * Error raised by the JavaScript storage facade.  This type is deliberately
 * engine-local: file connectors already expose the operation contract through
 * {@code TapFileStorage}, so the common API does not need a second operation
 * abstraction.
 */
public class StorageOperationException extends RuntimeException {

    public StorageOperationException(String message) {
        super(message);
    }

    public StorageOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
