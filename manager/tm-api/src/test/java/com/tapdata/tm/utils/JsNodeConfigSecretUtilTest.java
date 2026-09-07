package com.tapdata.tm.utils;

import com.tapdata.tm.commons.dag.process.script.JsNodeConfigParam;
import com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsNodeConfigSecretUtilTest {
    @Test
    void encryptsPlainValueWithTheEngineCompatibleFormat() {
        JsNodeConfigParam param = JsNodeConfigParam.builder()
                .key("mgm.password").type(JsNodeConfigValueType.STRING)
                .value("plain-password").encrypted(true).build();

        JsNodeConfigSecretUtil.encryptIfNeeded(param);

        assertNotEquals("plain-password", param.getValue());
        assertEquals("plain-password", AES256Util.Aes256Decode(String.valueOf(param.getValue())));
    }

    @Test
    void doesNotEncryptAnExistingCiphertextTwice() {
        JsNodeConfigParam param = JsNodeConfigParam.builder()
                .key("mgm.password").type(JsNodeConfigValueType.STRING)
                .value(AES256Util.Aes256Encode("plain-password")).encrypted(true).build();
        String ciphertext = String.valueOf(param.getValue());

        JsNodeConfigSecretUtil.encryptIfNeeded(param);

        assertEquals(ciphertext, param.getValue());
    }

    @Test
    void serializesEncryptedJsonBeforeEncryption() {
        JsNodeConfigParam param = JsNodeConfigParam.builder()
                .key("mgm.options").type(JsNodeConfigValueType.JSON)
                .value(Map.of("mode", "binary")).encrypted(true).build();

        JsNodeConfigSecretUtil.encryptIfNeeded(param);

        assertEquals("{\"mode\":\"binary\"}",
                AES256Util.Aes256Decode(String.valueOf(param.getValue())));
    }
}
