package com.tapdata.tm.ds.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersionDetector;
import com.networknt.schema.ValidationMessage;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import lombok.*;

import java.io.IOException;
import java.util.Set;

/**
 * @Author: Zed
 * @Date: 2021/8/26
 * @Description:
 */
public class JsonSchemaUtils {
    private static final ObjectMapper mapper = new ObjectMapper();

    private static JsonNode getJsonNodeFromStringContent(String content) throws IOException {
        return mapper.readTree(content);
    }

    // Automatically detect version for given JsonNode
    private static JsonSchema getJsonSchemaFromJsonNodeAutomaticVersion(JsonNode jsonNode) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersionDetector.detect(jsonNode));
        return factory.getSchema(jsonNode);
    }

    public static boolean checkDataSourceDefinition(DataSourceDefinitionDto definitionDto, Object config) {
        if (definitionDto.getProperties() == null) {
            return true;
        }

        try {
            JsonSchemaModel jsonSchemaModel = new JsonSchemaModel(definitionDto.getProperties());
            String schemaJson = mapper.writeValueAsString(jsonSchemaModel);
            schemaJson = schemaJson.replaceFirst("\"schema\"", "\"\\$schema\"");
            JsonNode schemaNode = getJsonNodeFromStringContent(schemaJson);
            JsonSchema schema = JsonSchemaUtils.getJsonSchemaFromJsonNodeAutomaticVersion(schemaNode);
            schema.initializeValidators();
            String nodeJson = mapper.writeValueAsString(config);
            JsonNode node = JsonSchemaUtils.getJsonNodeFromStringContent(nodeJson);
            Set<ValidationMessage> errors = schema.validate(node);
            if (errors.size() != 0) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }


    public static void main(String[] args) throws IOException {
        // With automatic version detection
        JsonNode schemaNode = JsonSchemaUtils.getJsonNodeFromStringContent(
                "{\"$schema\": \"http://json-schema.org/draft-06/schema#\", \"properties\": {\n" +
                        "        \"className\":{\n" +
                        "            \"type\":\"string\",\n" +
                        "            \"default\":\"com.streamsets.pipeline.stage.common.s3.AwsS3ConnectionVerifier\",\n" +
                        "            \"x-hidden\":true\n" +
                        "        },\n" +
                        "        \"libName\":{\n" +
                        "            \"type\":\"string\",\n" +
                        "            \"default\":\"aws\",\n" +
                        "            \"x-hidden\":true\n" +
                        "        },\n" +
                        "        \"type\":{\n" +
                        "            \"type\":\"string\",\n" +
                        "            \"default\":\"Source\",\n" +
                        "             \"required\": true\n" +
                        "        }\n" +
                        "    }\n" +
                        "}");
        JsonSchema schema1 = JsonSchemaUtils.getJsonSchemaFromJsonNodeAutomaticVersion(schemaNode);

        schema1.initializeValidators(); // by default all schemas are loaded lazily. You can load them eagerly via
        // initializeValidators()

        JsonNode node1 = JsonSchemaUtils.getJsonNodeFromStringContent("{\"className\": \"zed\", \"libName\" : \"12\"}");
        Set<ValidationMessage> errors1 = schema1.validate(node1);
        System.out.println(errors1);
        //assertThat(errors.size(), is(1));
    }

    @Data
    @EqualsAndHashCode(callSuper=false)
    public static class JsonSchemaModel {
        private String schema = "http://json-schema.org/draft-06/schema#";
        private Object properties;

        public JsonSchemaModel(Object properties) {
            this.properties = properties;
        }
    }
}
