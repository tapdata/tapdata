package io.tapdata.flow.engine.V2.script.storage;

/**
 * Defines how the JavaScript storage facade interprets {@code content}.
 */
public enum StorageContentType {
    TEXT("text", false),
    BYTES("bytes", false),
    STREAM("stream", true);

    private final String value;
    private final boolean callerOwned;

    StorageContentType(String value, boolean callerOwned) {
        this.value = value;
        this.callerOwned = callerOwned;
    }

    public String getValue() {
        return value;
    }

    public boolean isCallerOwned() {
        return callerOwned;
    }

    public static StorageContentType fromValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return TEXT;
        }
        String normalized = value.trim();
        for (StorageContentType contentType : values()) {
            if (contentType.value.equalsIgnoreCase(normalized)) {
                return contentType;
            }
        }
        throw new StorageOperationException(
                "Unsupported storage contentType: " + value + ". Supported values: text, bytes, stream");
    }
}
