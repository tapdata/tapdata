package com.tapdata.processor.js;

/** Stable, non-sensitive failure raised by the JS node configuration accessor. */
public class JsNodeConfigAccessException extends RuntimeException {
    private final String code;
    private final String key;

    public JsNodeConfigAccessException(String code, String key, String message) {
        super(message);
        this.code = code;
        this.key = key;
    }

    public JsNodeConfigAccessException(String code, String key, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.key = key;
    }

    public String getCode() {
        return code;
    }

    public String getKey() {
        return key;
    }
}
