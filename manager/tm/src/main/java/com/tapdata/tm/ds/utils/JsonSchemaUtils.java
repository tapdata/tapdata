package com.tapdata.tm.ds.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import lombok.*;

import java.io.IOException;
import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/8/26
 * @Description:
 */
public class JsonSchemaUtils {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final tools.jackson.databind.ObjectMapper schemaMapper = new tools.jackson.databind.ObjectMapper();

    private static tools.jackson.databind.JsonNode getJsonNodeFromStringContent(String content) throws IOException {
        return schemaMapper.readTree(content);
    }

    // Automatically detect version for given JsonNode
    private static Schema getJsonSchemaFromJsonNodeAutomaticVersion(tools.jackson.databind.JsonNode jsonNode) {
        SchemaRegistry registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_6);
        return registry.getSchema(jsonNode);
    }

    public static boolean checkDataSourceDefinition(DataSourceDefinitionDto definitionDto, Object config) {
        if (definitionDto.getProperties() == null) {
            return true;
        }

        try {
            JsonSchemaModel jsonSchemaModel = new JsonSchemaModel(definitionDto.getProperties());
            String schemaJson = mapper.writeValueAsString(jsonSchemaModel);
            schemaJson = schemaJson.replaceFirst("\"schema\"", "\"\\$schema\"");
            tools.jackson.databind.JsonNode schemaNode = getJsonNodeFromStringContent(schemaJson);
            Schema schema = JsonSchemaUtils.getJsonSchemaFromJsonNodeAutomaticVersion(schemaNode);
            schema.initializeValidators();
            String nodeJson = mapper.writeValueAsString(config);
            tools.jackson.databind.JsonNode node = JsonSchemaUtils.getJsonNodeFromStringContent(nodeJson);
            List<Error> errors = schema.validate(node);
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
        tools.jackson.databind.JsonNode schemaNode = JsonSchemaUtils.getJsonNodeFromStringContent(
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
        Schema schema1 = JsonSchemaUtils.getJsonSchemaFromJsonNodeAutomaticVersion(schemaNode);

        schema1.initializeValidators(); // by default all schemas are loaded lazily. You can load them eagerly via
        // initializeValidators()

        tools.jackson.databind.JsonNode node1 = JsonSchemaUtils.getJsonNodeFromStringContent("{\"className\": \"zed\", \"libName\" : \"12\"}");
        List<Error> errors1 = schema1.validate(node1);
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
