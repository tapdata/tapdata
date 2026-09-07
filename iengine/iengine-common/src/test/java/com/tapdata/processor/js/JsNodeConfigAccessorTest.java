package com.tapdata.processor.js;

import com.tapdata.tm.commons.dag.process.script.JsNodeConfigParam;
import com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsNodeConfigAccessorTest {

    @Test
    void readsExactKeysAndDefaultsWithoutExposingTheBackingMap() {
        DefaultJsNodeConfigAccessor accessor = new DefaultJsNodeConfigAccessor(Arrays.asList(
                JsNodeConfigParam.builder().key("mgm.host").type(JsNodeConfigValueType.STRING).value("ftp.example").build(),
                JsNodeConfigParam.builder().key("mgm.port").type(JsNodeConfigValueType.NUMBER).value(21).build(),
                JsNodeConfigParam.builder().key("mgm.options").type(JsNodeConfigValueType.JSON)
                        .value("{\"mode\":\"binary\"}").build()));

        assertTrue(accessor.has("mgm.host"));
        assertEquals("ftp.example", accessor.get("mgm.host"));
        assertEquals(21, ((Number) accessor.get("mgm.port")).intValue());
        assertEquals("binary", ((Map<?, ?>) accessor.get("mgm.options")).get("mode"));
        assertEquals(21, accessor.getOrDefault("mgm.port", 22));
        assertEquals(22, accessor.getOrDefault("missing", 22));
        assertThrows(JsNodeConfigAccessException.class, () -> accessor.get("missing"));
    }

    @Test
    void encryptedValueRequiresPermissionAndUsesInjectedResolver() {
        JsNodeConfigParam secret = JsNodeConfigParam.builder().key("mgm.password")
                .type(JsNodeConfigValueType.STRING).value("ciphertext").encrypted(true).build();
        DefaultJsNodeConfigAccessor denied = new DefaultJsNodeConfigAccessor(Collections.singletonList(secret));
        JsNodeConfigAccessException deniedError = assertThrows(JsNodeConfigAccessException.class,
                () -> denied.get("mgm.password"));
        assertEquals("JS_NODE_CONFIG_SECRET_FORBIDDEN", deniedError.getCode());
        assertFalse(deniedError.getMessage().contains("ciphertext"));

        DefaultJsNodeConfigAccessor allowed = new DefaultJsNodeConfigAccessor(Collections.singletonList(secret),
                (key, value) -> {
                    assertEquals("mgm.password", key);
                    assertEquals("ciphertext", value);
                    return "plain-password";
                });
        assertEquals("plain-password", allowed.get("mgm.password"));
    }

    @Test
    void malformedJsonIsRejectedAtAccessorConstruction() {
        JsNodeConfigParam invalid = JsNodeConfigParam.builder().key("mgm.options")
                .type(JsNodeConfigValueType.JSON).value("{").build();
        JsNodeConfigAccessException error = assertThrows(JsNodeConfigAccessException.class,
                () -> new DefaultJsNodeConfigAccessor(Collections.singletonList(invalid)));
        assertEquals("JS_NODE_CONFIG_TYPE_INVALID", error.getCode());
    }
}
