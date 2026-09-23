package io.tapdata.flow.engine.V2.script.storage;

/**
 * Defines how the JavaScript storage facade interprets {@code content}.
 */
public enum StorageContentType {
    TEXT("text"),
    BYTES("bytes"),
    STREAM("stream");

    private final String value;

    StorageContentType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
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
