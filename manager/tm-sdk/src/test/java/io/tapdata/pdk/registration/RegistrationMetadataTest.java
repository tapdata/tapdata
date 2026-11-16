package io.tapdata.pdk.registration;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RegistrationMetadataTest {
    private TapNodeInfo node(String type, String json) {
        TapNodeSpecification specification = new TapNodeSpecification();
        specification.setId("test");
        specification.setName("Test");
        specification.setManifest(new HashMap<>(Map.of("Authentication", "GA", "PDK-API-Version", "2.0.0")));
        TapNodeInfo node = mock(TapNodeInfo.class);
        when(node.getTapNodeSpecification()).thenReturn(specification);
        when(node.getNodeType()).thenReturn(type);
        doReturn(MetadataFixtureConnector.class).when(node).getNodeClass();
        when(node.readResource("annotated-spec.json")).thenAnswer(invocation ->
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        return node;
    }

    @Test
    void preservesTypesAndUsesDefaultDataTypes() throws Exception {
        Map<String, String> types = Map.of(TapNodeInfo.NODE_TYPE_SOURCE, "source",
                TapNodeInfo.NODE_TYPE_TARGET, "target", TapNodeInfo.NODE_TYPE_SOURCE_TARGET, "source_and_target",
                TapNodeInfo.NODE_TYPE_PROCESSOR, "processor");
        for (Map.Entry<String, String> type : types.entrySet()) {
            JSONObject payload = RegistrationMetadata.build(node(type.getKey(), "{}"), path -> fail(path));
            assertEquals(type.getValue(), payload.getString("type"));
            assertFalse(JSON.parseObject(payload.getString("expression")).isEmpty());
            assertTrue(payload.getString("tapTypeDataTypeMap").contains("varchar"));
            assertEquals("GA", payload.getString("qcType"));
        }
        assertThrows(IllegalArgumentException.class,
                () -> RegistrationMetadata.build(node("unknown", "{}"), path -> {}));
    }

    @Test
    void mergesRuntimeSpecAndDdlCapabilitiesAndOnlyUsesReferencedDocs() throws Exception {
        String json = "{\"dataTypes\":{\"custom\":{}},\"configOptions\":{"
                + "\"capabilities\":[{\"id\":\"policy\",\"alternatives\":[\"ignore\"]},{}],"
                + "\"supportDDL\":{\"events\":[\"new_field_event\"]}},\"messages\":{\"default\":\"en\","
                + "\"empty\":null,\"en\":{\"doc\":\"readme.md\",\"doc:more\":\"more.md\","
                + "\"doc:empty\":\"\",\"doc:null\":null,\"title\":\"Title\"}}}";
        List<String> docs = new ArrayList<>();
        JSONObject payload = RegistrationMetadata.build(node(TapNodeInfo.NODE_TYPE_SOURCE, json), docs::add);
        assertEquals(2, docs.size());
        assertTrue(docs.containsAll(List.of("readme.md", "more.md")));
        assertTrue(JSON.parseObject(payload.getString("expression")).containsKey("custom"));
        assertEquals("readme.md", payload.getJSONObject("messages").getJSONObject("en").getString("doc"));
        String capabilities = payload.getJSONArray("capabilities").toJSONString();
        assertTrue(capabilities.contains("policy"));
        assertTrue(capabilities.contains("ignore"));
        assertTrue(capabilities.contains("new_field_event"));
        assertTrue(payload.getJSONArray("capabilities").size() > 2);
    }

    @Test
    void preservesOptionalConfigAndAuthenticationBehavior() throws Exception {
        for (String json : List.of("{\"configOptions\":{}}", "{\"configOptions\":{\"supportDDL\":{}}}")) {
            TapNodeInfo node = node(TapNodeInfo.NODE_TYPE_TARGET, json);
            node.getTapNodeSpecification().getManifest().remove("Authentication");
            assertEquals("Alpha", RegistrationMetadata.build(node, path -> {}).getString("qcType"));
            node.getTapNodeSpecification().getManifest().put("Authentication", "beta");
            assertTrue(RegistrationMetadata.build(node, path -> {}).getBoolean("beta"));
        }
    }

}
