package com.tapdata.processor.js;

import com.tapdata.constant.AES256Util;

/** Adapter to the engine's existing task configuration encryption format. */
public final class Aes256JsNodeConfigSecretResolver implements JsNodeConfigSecretResolver {
    @Override
    public String decrypt(String key, String encryptedValue) {
        if (encryptedValue == null) return null;
        return AES256Util.Aes256Decode(encryptedValue, AES256Util.getKey());
    }
}
