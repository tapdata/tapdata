package com.tapdata.processor.js;

/** Resolves an encrypted script parameter only inside an authorized task runtime. */
@FunctionalInterface
public interface JsNodeConfigSecretResolver {
    String decrypt(String key, String encryptedValue);
}
