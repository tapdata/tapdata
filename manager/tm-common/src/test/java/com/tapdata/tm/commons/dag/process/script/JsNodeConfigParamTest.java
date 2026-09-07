package com.tapdata.tm.commons.dag.process.script;

import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsNodeConfigParamTest {

    @Test
    void newScriptNodesHaveEmptyConfigAndRoundTripParams() {
        JsProcessorNode node = new JsProcessorNode();
        assertNotNull(node.getScriptParams());
        assertTrue(node.getScriptParams().isEmpty());

        node.setScriptParams(Collections.singletonList(
                JsNodeConfigParam.builder()
                        .key("mgm.in.port")
                        .type(JsNodeConfigValueType.NUMBER)
                        .value(21)
                        .encrypted(false)
                        .description("FTP port")
                        .build()));

        String json = JsonUtil.toJsonUseJackson(node);
        JsProcessorNode restored = JsonUtil.parseJsonUseJackson(json, JsProcessorNode.class);
        assertNotNull(restored);
        assertEquals(1, restored.getScriptParams().size());
        assertEquals("mgm.in.port", restored.getScriptParams().get(0).getKey());
        assertEquals(JsNodeConfigValueType.NUMBER, restored.getScriptParams().get(0).getType());
        assertEquals(21, ((Number) restored.getScriptParams().get(0).getValue()).intValue());
    }

    @Test
    void migrationScriptNodesPersistTheSameConfigField() {
        MigrateJsProcessorNode node = new MigrateJsProcessorNode();
        node.setScriptParams(Collections.singletonList(
                JsNodeConfigParam.builder().key("mgm.secret").type(JsNodeConfigValueType.STRING)
                        .value("ciphertext").encrypted(true).build()));

        String json = JsonUtil.toJsonUseJackson(node);
        MigrateJsProcessorNode restored = JsonUtil.parseJsonUseJackson(json, MigrateJsProcessorNode.class);
        assertEquals("mgm.secret", restored.getScriptParams().get(0).getKey());
        assertTrue(restored.getScriptParams().get(0).isEncrypted());
    }

    @Test
    void valueTypeAcceptsCaseInsensitiveJsonNames() {
        assertEquals(JsNodeConfigValueType.STRING,
                JsonUtil.parseJsonUseJackson("\"string\"", JsNodeConfigValueType.class));
        assertEquals(JsNodeConfigValueType.JSON,
                JsonUtil.parseJsonUseJackson("\"JSON\"", JsNodeConfigValueType.class));
    }

    @Test
    void validatorReturnsFieldLevelErrorsAndAcceptsSupportedValues() {
        List<JsNodeConfigParam> valid = Arrays.asList(
                JsNodeConfigParam.builder().key("mgm.host").type(JsNodeConfigValueType.STRING).value("ftp.example").build(),
                JsNodeConfigParam.builder().key("mgm.port").type(JsNodeConfigValueType.NUMBER).value(21).build(),
                JsNodeConfigParam.builder().key("mgm.passive").type(JsNodeConfigValueType.BOOLEAN).value(true).build(),
                JsNodeConfigParam.builder().key("mgm.options").type(JsNodeConfigValueType.JSON).value("{\"mode\":\"binary\"}").build());
        assertTrue(JsNodeConfigValidator.validate(valid).isEmpty());

        List<JsNodeConfigParam> invalid = Arrays.asList(
                JsNodeConfigParam.builder().key("mgm.host").type(JsNodeConfigValueType.STRING).value("a").build(),
                JsNodeConfigParam.builder().key("mgm.host").type(JsNodeConfigValueType.STRING).value("b").build(),
                JsNodeConfigParam.builder().key("tapdata.internal").type(JsNodeConfigValueType.STRING).value("x").build(),
                JsNodeConfigParam.builder().key("mgm.port").type(JsNodeConfigValueType.NUMBER).value("21").build(),
                JsNodeConfigParam.builder().key("mgm.bad json").type(JsNodeConfigValueType.JSON).value("{").build());
        List<JsNodeConfigValidationError> errors = JsNodeConfigValidator.validate(invalid);
        assertTrue(errors.stream().anyMatch(e -> "DUPLICATE_KEY".equals(e.getCode())));
        assertTrue(errors.stream().anyMatch(e -> "KEY_RESERVED".equals(e.getCode())));
        assertTrue(errors.stream().anyMatch(e -> "TYPE_INVALID".equals(e.getCode())));
        assertTrue(errors.stream().anyMatch(e -> "JSON_INVALID".equals(e.getCode())));
    }

    @Test
    void validatorRejectsNullAndOversizedDefinitions() {
        assertFalse(JsNodeConfigValidator.validate(null).isEmpty());
        JsNodeConfigParam param = JsNodeConfigParam.builder()
                .key("mgm.secret").type(JsNodeConfigValueType.STRING)
                .value(new String(new char[70 * 1024]).replace('\0', 'x')).build();
        assertTrue(JsNodeConfigValidator.validate(Collections.singletonList(param)).stream()
                .anyMatch(e -> "VALUE_TOO_LARGE".equals(e.getCode())));
    }

    @Test
    void scriptNodesRejectInvalidParametersDuringNodeValidation() {
        JsProcessorNode node = new JsProcessorNode();
        node.setScriptParams(Collections.singletonList(
                JsNodeConfigParam.builder().key("mgm.port").type(JsNodeConfigValueType.NUMBER)
                        .value("21").build()));
        assertFalse(node.validate());

        MigrateJsProcessorNode migrateNode = new MigrateJsProcessorNode();
        migrateNode.setScriptParams(Collections.singletonList(
                JsNodeConfigParam.builder().key("mgm.port").type(JsNodeConfigValueType.NUMBER)
                        .value(21).build()));
        assertTrue(migrateNode.validate());
    }
}
