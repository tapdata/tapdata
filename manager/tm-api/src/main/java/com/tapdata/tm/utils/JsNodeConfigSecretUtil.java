package com.tapdata.tm.utils;

import com.tapdata.tm.commons.dag.process.script.JsNodeConfigParam;
import com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType;
import com.tapdata.tm.commons.util.JsonUtil;

import java.util.regex.Pattern;

/**
 * Persists encrypted JS node parameters using the same AES-256 format consumed
 * by the engine. Values that already contain a valid ciphertext are preserved
 * so an edit of an unrelated node property does not encrypt them twice.
 */
public final class JsNodeConfigSecretUtil {
    private static final Pattern HEX_CIPHERTEXT = Pattern.compile("[0-9a-fA-F]+");

    private JsNodeConfigSecretUtil() {
    }

    public static void encryptIfNeeded(JsNodeConfigParam param) {
        if (param == null || !param.isEncrypted() || param.getValue() == null) {
            return;
        }
        String text = serialize(param.getType(), param.getValue());
        if (!isCiphertext(text)) {
            param.setValue(AES256Util.Aes256Encode(text));
        }
    }

    public static boolean isCiphertext(String value) {
        if (value == null || value.length() == 0 || value.length() % 32 != 0
                || !HEX_CIPHERTEXT.matcher(value).matches()) {
            return false;
        }
        String decoded = AES256Util.Aes256Decode(value);
        return !value.equals(decoded);
    }

    private static String serialize(JsNodeConfigValueType type, Object value) {
        if (type == JsNodeConfigValueType.JSON && !(value instanceof CharSequence)) {
            return JsonUtil.toJsonUseJackson(value);
        }
        return String.valueOf(value);
    }
}
