package com.tapdata.tm.commons.dag.process.script;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/** Pure validation for values persisted in ScriptProcessNode.scriptParams. */
public final class JsNodeConfigValidator {
    public static final int MAX_PARAMS = 100;
    public static final int MAX_VALUE_BYTES = 64 * 1024;
    public static final int MAX_TOTAL_BYTES = 512 * 1024;
    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z0-9_./:-]{1,128}");
    private static final ObjectMapper JSON = new ObjectMapper();

    private JsNodeConfigValidator() {
    }

    public static List<JsNodeConfigValidationError> validate(List<JsNodeConfigParam> params) {
        List<JsNodeConfigValidationError> errors = new ArrayList<>();
        if (params == null) {
            errors.add(error(-1, null, "PARAMS_REQUIRED", "scriptParams must not be null"));
            return errors;
        }
        if (params.size() > MAX_PARAMS) {
            errors.add(error(-1, null, "PARAM_COUNT_EXCEEDED", "scriptParams exceeds the maximum number of parameters"));
        }
        Set<String> keys = new HashSet<>();
        int totalBytes = 0;
        for (int index = 0; index < params.size(); index++) {
            JsNodeConfigParam param = params.get(index);
            if (param == null) {
                errors.add(error(index, null, "PARAM_REQUIRED", "parameter must not be null"));
                continue;
            }
            String key = param.getKey();
            if (key == null || key.trim().isEmpty()) {
                errors.add(error(index, key, "KEY_REQUIRED", "key must not be blank"));
            } else {
                if (!KEY_PATTERN.matcher(key).matches()) {
                    errors.add(error(index, key, "KEY_INVALID", "key contains unsupported characters or exceeds 128 characters"));
                }
                if (key.startsWith("tapdata.")) {
                    errors.add(error(index, key, "KEY_RESERVED", "key uses a reserved prefix"));
                }
                if (!keys.add(key)) {
                    errors.add(error(index, key, "DUPLICATE_KEY", "key must be unique"));
                }
            }
            JsNodeConfigValueType type = param.getType();
            if (type == null) {
                errors.add(error(index, key, "TYPE_REQUIRED", "type must be specified"));
            }
            Object value = param.getValue();
            if (value == null) {
                errors.add(error(index, key, "VALUE_REQUIRED", "value must not be null"));
                continue;
            }
            String serialized = String.valueOf(value);
            int valueBytes = serialized.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            totalBytes += valueBytes;
            if (valueBytes > MAX_VALUE_BYTES) {
                errors.add(error(index, key, "VALUE_TOO_LARGE", "value exceeds 64 KB"));
            }
            if (param.isEncrypted() && !(value instanceof String)) {
                errors.add(error(index, key, "ENCRYPTED_VALUE_INVALID", "encrypted value must be a string"));
            } else if (type != null && !param.isEncrypted()) {
                validateType(index, key, type, value, errors);
            }
        }
        if (totalBytes > MAX_TOTAL_BYTES) {
            errors.add(error(-1, null, "TOTAL_VALUE_TOO_LARGE", "scriptParams values exceed 512 KB"));
        }
        return errors;
    }

    private static void validateType(int index, String key, JsNodeConfigValueType type, Object value,
                                     List<JsNodeConfigValidationError> errors) {
        switch (type) {
            case STRING:
                if (!(value instanceof CharSequence)) {
                    errors.add(error(index, key, "TYPE_INVALID", "STRING value must be text"));
                }
                break;
            case NUMBER:
                if (!(value instanceof Number)) {
                    errors.add(error(index, key, "TYPE_INVALID", "NUMBER value must be numeric"));
                }
                break;
            case BOOLEAN:
                if (!(value instanceof Boolean)) {
                    errors.add(error(index, key, "TYPE_INVALID", "BOOLEAN value must be true or false"));
                }
                break;
            case JSON:
                if (value instanceof CharSequence) {
                    try {
                        JSON.readTree(value.toString());
                    } catch (Exception e) {
                        errors.add(error(index, key, "JSON_INVALID", "JSON value cannot be parsed"));
                    }
                } else if (!(value instanceof java.util.Map) && !(value instanceof java.util.Collection)
                        && !(value instanceof Number) && !(value instanceof Boolean)) {
                    errors.add(error(index, key, "JSON_INVALID", "JSON value must be valid JSON"));
                }
                break;
            default:
                errors.add(error(index, key, "TYPE_INVALID", "unsupported value type"));
        }
    }

    private static JsNodeConfigValidationError error(int index, String key, String code, String message) {
        return new JsNodeConfigValidationError(index, key, code, message);
    }
}
