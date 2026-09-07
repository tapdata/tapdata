package com.tapdata.processor.js;

/** Narrow runtime API exposed to GraalJS as {@code jsNodeConfig}. */
public interface JsNodeConfigAccessor {
    Object get(String key);

    boolean has(String key);

    Object getOrDefault(String key, Object defaultValue);
}
