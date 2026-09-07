package com.tapdata.processor.js;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.dag.process.script.JsNodeConfigParam;
import com.tapdata.tm.commons.dag.process.script.JsNodeConfigValidationError;
import com.tapdata.tm.commons.dag.process.script.JsNodeConfigValidator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Default exact-key accessor. It never exposes an operation that exports all secrets. */
public final class DefaultJsNodeConfigAccessor implements JsNodeConfigAccessor {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Map<String, Entry> entries;
    private final JsNodeConfigSecretResolver secretResolver;

    public DefaultJsNodeConfigAccessor(List<JsNodeConfigParam> params) {
        this(params, null);
    }

    public DefaultJsNodeConfigAccessor(List<JsNodeConfigParam> params, JsNodeConfigSecretResolver secretResolver) {
        List<JsNodeConfigParam> safeParams = params == null ? Collections.emptyList() : params;
        List<JsNodeConfigValidationError> errors = JsNodeConfigValidator.validate(safeParams);
        if (!errors.isEmpty()) {
            JsNodeConfigValidationError error = errors.get(0);
            throw new JsNodeConfigAccessException("JS_NODE_CONFIG_TYPE_INVALID", error.getKey(),
                    "invalid JS node config parameter: " + error.getCode());
        }
        this.secretResolver = secretResolver;
        Map<String, Entry> values = new LinkedHashMap<>();
        for (JsNodeConfigParam param : safeParams) {
            Object value = param.getValue();
            if (param.isEncrypted()) {
                values.put(param.getKey(), Entry.encrypted(param.getType(), String.valueOf(value)));
            } else {
                values.put(param.getKey(), Entry.plain(param.getType(), normalize(param.getType(), value, param.getKey())));
            }
        }
        this.entries = Collections.unmodifiableMap(values);
    }

    @Override
    public Object get(String key) {
        Entry entry = entries.get(key);
        if (entry == null) {
            throw new JsNodeConfigAccessException("JS_NODE_CONFIG_KEY_NOT_FOUND", key,
                    "JS node config key not found: " + safeKey(key));
        }
        if (!entry.encrypted) return entry.value;
        if (secretResolver == null) {
            throw new JsNodeConfigAccessException("JS_NODE_CONFIG_SECRET_FORBIDDEN", key,
                    "secret access is not permitted for JS node config key: " + safeKey(key));
        }
        try {
            String decrypted = secretResolver.decrypt(key, (String) entry.value);
            if (decrypted == null) {
                throw new IllegalStateException("secret resolver returned null");
            }
            return normalize(entry.type, decrypted, key);
        } catch (JsNodeConfigAccessException e) {
            throw e;
        } catch (Exception e) {
            throw new JsNodeConfigAccessException("JS_NODE_CONFIG_SECRET_DECRYPT_FAILED", key,
                    "failed to resolve secret for JS node config key: " + safeKey(key), e);
        }
    }

    @Override
    public boolean has(String key) {
        return entries.containsKey(key);
    }

    @Override
    public Object getOrDefault(String key, Object defaultValue) {
        return has(key) ? get(key) : defaultValue;
    }

    /** Internal bridge for the Java file facade; this method is never injected into GraalJS. */
    Map<String, Object> snapshotByPrefix(String prefix) {
        if (prefix == null || prefix.trim().isEmpty()) {
            throw new JsNodeConfigAccessException("JS_NODE_CONFIG_KEY_NOT_FOUND", prefix,
                    "config prefix must not be blank");
        }
        String normalized = prefix.endsWith(".") ? prefix.substring(0, prefix.length() - 1) : prefix;
        Map<String, Object> result = new LinkedHashMap<>();
        for (String key : entries.keySet()) {
            if (key.equals(normalized) || key.startsWith(normalized + ".")) {
                String suffix = key.equals(normalized) ? "" : key.substring(normalized.length() + 1);
                result.put(suffix, get(key));
            }
        }
        if (result.isEmpty()) {
            throw new JsNodeConfigAccessException("JS_NODE_CONFIG_KEY_NOT_FOUND", prefix,
                    "JS node config prefix not found: " + safeKey(prefix));
        }
        return result;
    }

    private static Object normalize(com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType type,
                                    Object value, String key) {
        if (!(value instanceof CharSequence)) return value;
        String text = value.toString();
        try {
            switch (type) {
                case JSON:
                    return JSON.readValue(text, Object.class);
                case NUMBER:
                    if (text.contains(".")) return Double.parseDouble(text);
                    long integer = Long.parseLong(text);
                    return integer <= Integer.MAX_VALUE && integer >= Integer.MIN_VALUE
                            ? (int) integer : integer;
                case BOOLEAN:
                    if ("true".equalsIgnoreCase(text)) return true;
                    if ("false".equalsIgnoreCase(text)) return false;
                    throw new IllegalArgumentException("boolean must be true or false");
                case STRING:
                default:
                    return text;
            }
        } catch (Exception e) {
            throw new JsNodeConfigAccessException("JS_NODE_CONFIG_TYPE_INVALID", key,
                    "invalid " + type.name().toLowerCase() + " value for JS node config key: " + safeKey(key), e);
        }
    }

    private static String safeKey(String key) {
        return key == null ? "<null>" : key;
    }

    static final class Entry {
        private final com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType type;
        private final Object value;
        private final boolean encrypted;

        private Entry(com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType type, Object value, boolean encrypted) {
            this.type = type;
            this.value = value;
            this.encrypted = encrypted;
        }

        static Entry plain(com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType type, Object value) {
            return new Entry(type, value, false);
        }

        static Entry encrypted(com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType type, String value) {
            return new Entry(type, value, true);
        }
    }
}
