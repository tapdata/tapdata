package com.tapdata.tm.commons.dag.process.script;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

/** Supported value types for a JS node's user-defined configuration. */
public enum JsNodeConfigValueType {
    STRING("string"),
    NUMBER("number"),
    BOOLEAN("boolean"),
    JSON("json");

    private final String value;

    JsNodeConfigValueType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static JsNodeConfigValueType fromValue(String value) {
        if (value == null) return null;
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (JsNodeConfigValueType type : values()) {
            if (type.value.equals(normalized) || type.name().equalsIgnoreCase(normalized)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported JS node config value type: " + value);
    }
}
