package com.tapdata.processor.js;

/** Public GraalJS view of the configuration accessor. Only exact-key methods are exposed. */
public final class JsNodeConfigScriptFacade {
    private final JsNodeConfigAccessor delegate;

    public JsNodeConfigScriptFacade(JsNodeConfigAccessor delegate) {
        this.delegate = delegate;
    }

    public Object get(String key) {
        return delegate.get(key);
    }

    public boolean has(String key) {
        return delegate.has(key);
    }

    public Object getOrDefault(String key, Object defaultValue) {
        return delegate.getOrDefault(key, defaultValue);
    }
}
