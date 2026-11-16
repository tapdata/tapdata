package io.tapdata.pdk.registration;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.constants.DataSourceQCType;
import io.tapdata.pdk.core.tapnode.TapNodeContainer;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** Registration payload shared by CLI upload and release download. */
public final class RegistrationMetadata {
    private RegistrationMetadata() {
    }

    public static JSONObject build(TapNodeInfo nodeInfo, Consumer<String> documentation) throws Exception {
        TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
        JSONObject payload = (JSONObject) JSON.toJSON(specification);
        String authentication = specification.getManifest().get("Authentication");
        DataSourceQCType qcType = DataSourceQCType.parse(authentication);
        payload.put("qcType", qcType == null ? DataSourceQCType.Alpha : qcType);
        String apiVersion = specification.getManifest().get("PDK-API-Version");
        payload.put("pdkAPIVersion", apiVersion);
        payload.put("pdkAPIBuildNumber", CommonUtils.getPdkBuildNumer(apiVersion));
        payload.put("beta", "beta".equals(authentication));
        payload.put("type", nodeType(nodeInfo.getNodeType()));
        payload.put("version", nodeInfo.getNodeClass().getPackage().getImplementationVersion());
        payload.put("group", nodeInfo.getNodeClass().getPackage().getImplementationVendor());

        TapNodeContainer container = readContainer(nodeInfo);
        collectDocumentation(container.getMessages(), documentation);
        payload.put("expression", JSON.toJSONString(container.getDataTypes()));
        if (container.getMessages() != null) {
            payload.put("messages", container.getMessages());
        }
        addCapabilities(payload, nodeInfo, container.getConfigOptions());
        return payload;
    }

    private static String nodeType(String type) {
        switch (type) {
            case TapNodeInfo.NODE_TYPE_SOURCE: return "source";
            case TapNodeInfo.NODE_TYPE_SOURCE_TARGET: return "source_and_target";
            case TapNodeInfo.NODE_TYPE_TARGET: return "target";
            case TapNodeInfo.NODE_TYPE_PROCESSOR: return "processor";
            default: throw new IllegalArgumentException("Unknown node type " + type);
        }
    }

    private static TapNodeContainer readContainer(TapNodeInfo nodeInfo) throws Exception {
        TapNodeContainer container;
        String path = nodeInfo.getNodeClass().getAnnotation(TapConnectorClass.class).value();
        try (InputStream spec = nodeInfo.readResource(path)) {
            container = JSON.parseObject(IOUtils.toString(spec), TapNodeContainer.class);
        }
        if (container.getDataTypes() == null) {
            addDefaultDataTypes(container);
        }
        return container;
    }

    private static void addDefaultDataTypes(TapNodeContainer container) throws Exception {
        try (InputStream input = RegistrationMetadata.class.getResourceAsStream("/default-data-types.json")) {
            if (input == null) return;
            String json = org.apache.commons.io.IOUtils.toString(input, StandardCharsets.UTF_8);
            if (StringUtils.isBlank(json)) return;
            TapNodeContainer defaults = InstanceFactory.instance(JsonParser.class).fromJson(json, TapNodeContainer.class);
            if (defaults != null && defaults.getDataTypes() != null) {
                container.setDataTypes(defaults.getDataTypes());
            }
        }
    }

    private static void addCapabilities(JSONObject payload, TapNodeInfo nodeInfo, DataMap config) throws Exception {
        TapConnector connector = (TapConnector) nodeInfo.getNodeClass().getConstructor().newInstance();
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry registry = new TapCodecsRegistry();
        connector.registerCapabilities(functions, registry);
        List<Capability> capabilities = functions.getCapabilities();
        if (config != null) {
            addSpecCapabilities(capabilities, config);
            addDdlCapabilities(capabilities, config);
        }
        if (capabilities != null && !capabilities.isEmpty()) {
            payload.put("capabilities", capabilities);
            if (config != null) config.remove("capabilities");
        }
        payload.put("tapTypeDataTypeMap", JSON.toJSONString(registry.getTapTypeDataTypeMap()));
    }

    @SuppressWarnings("unchecked")
    private static void addSpecCapabilities(List<Capability> capabilities, DataMap config) {
        List<Map<String, Object>> specs = (List<Map<String, Object>>) config.get("capabilities");
        if (specs == null) return;
        for (Map<String, Object> spec : specs) {
            String id = (String) spec.get("id");
            if (id != null) {
                List<String> alternatives = (List<String>) spec.get("alternatives");
                capabilities.add(Capability.create(id).alternatives(alternatives).type(Capability.TYPE_OTHER));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void addDdlCapabilities(List<Capability> capabilities, DataMap config) {
        Map<String, Object> ddl = (Map<String, Object>) config.get("supportDDL");
        if (ddl == null) return;
        List<String> events = (List<String>) ddl.get("events");
        if (events == null) return;
        for (String event : events) {
            capabilities.add(Capability.create(event).type(Capability.TYPE_DDL));
        }
    }

    @SuppressWarnings("unchecked")
    private static void collectDocumentation(Map<String, Object> messages, Consumer<String> documentation) {
        if (messages == null) return;
        for (Map.Entry<String, Object> language : messages.entrySet()) {
            if (!"default".equalsIgnoreCase(language.getKey()) && language.getValue() != null) {
                collectLocalizedDocumentation((Map<String, Object>) language.getValue(), documentation);
            }
        }
    }

    private static void collectLocalizedDocumentation(Map<String, Object> localized, Consumer<String> documentation) {
        for (Map.Entry<String, Object> message : localized.entrySet()) {
            if ("doc".equals(message.getKey()) || message.getKey().startsWith("doc:")) {
                String path = (String) message.getValue();
                if (StringUtils.isNotEmpty(path)) documentation.accept(path);
            }
        }
    }
}
