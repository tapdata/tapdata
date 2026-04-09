package com.tapdata.tm.group.handler;

import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ResourceHandler i18n Label Resolution Tests")
public class ResourceHandlerI18nTest {

    private DataSourceDefinitionDto buildDefinition(Map<String, Object> fieldMeta,
                                                     LinkedHashMap<String, Object> messages) {
        // Build connection.properties structure
        LinkedHashMap<String, Object> connectionProperties = new LinkedHashMap<>();
        connectionProperties.put("testField", fieldMeta);

        LinkedHashMap<String, Object> connectionMeta = new LinkedHashMap<>();
        connectionMeta.put("properties", connectionProperties);

        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        properties.put("connection", connectionMeta);

        DataSourceDefinitionDto definition = new DataSourceDefinitionDto();
        definition.setProperties(properties);
        definition.setMessages(messages);
        return definition;
    }

    @Nested
    @DisplayName("buildConfigPathToLabelMap i18n resolution")
    class BuildConfigPathToLabelMapI18nTest {

        @Test
        @DisplayName("resolves ${key} placeholder using en_US messages")
        void resolvesPlaceholder() {
            Map<String, Object> fieldMeta = Map.of("title", "${logPluginName}");
            LinkedHashMap<String, Object> enMap = new LinkedHashMap<>();
            enMap.put("logPluginName", "Log Plugin");
            LinkedHashMap<String, Object> messages = new LinkedHashMap<>();
            messages.put("en_US", enMap);

            DataSourceDefinitionDto def = buildDefinition(new LinkedHashMap<>(fieldMeta), messages);
            Map<String, String> result = ResourceHandler.buildConfigPathToLabelMap(def);

            assertEquals("Log Plugin", result.get("testField"));
        }

        @Test
        @DisplayName("skips field when translation key is missing")
        void skipsWhenTranslationMissing() {
            Map<String, Object> fieldMeta = Map.of("title", "${missingKey}");
            LinkedHashMap<String, Object> enMap = new LinkedHashMap<>();
            LinkedHashMap<String, Object> messages = new LinkedHashMap<>();
            messages.put("en_US", enMap);

            DataSourceDefinitionDto def = buildDefinition(new LinkedHashMap<>(fieldMeta), messages);
            Map<String, String> result = ResourceHandler.buildConfigPathToLabelMap(def);

            assertFalse(result.containsKey("testField"));
        }

        @Test
        @DisplayName("keeps plain text title as-is")
        void keepsPlainTextTitle() {
            Map<String, Object> fieldMeta = Map.of("title", "Host");
            LinkedHashMap<String, Object> messages = new LinkedHashMap<>();

            DataSourceDefinitionDto def = buildDefinition(new LinkedHashMap<>(fieldMeta), messages);
            Map<String, String> result = ResourceHandler.buildConfigPathToLabelMap(def);

            assertEquals("Host", result.get("testField"));
        }

        @Test
        @DisplayName("skips placeholder when messages is null")
        void skipsWhenMessagesNull() {
            Map<String, Object> fieldMeta = Map.of("title", "${someKey}");

            DataSourceDefinitionDto def = buildDefinition(new LinkedHashMap<>(fieldMeta), null);
            Map<String, String> result = ResourceHandler.buildConfigPathToLabelMap(def);

            assertFalse(result.containsKey("testField"));
        }

        @Test
        @DisplayName("resolves multiple placeholders in one title")
        void resolvesMultiplePlaceholders() {
            Map<String, Object> fieldMeta = Map.of("title", "${prefix} - ${suffix}");
            LinkedHashMap<String, Object> enMap = new LinkedHashMap<>();
            enMap.put("prefix", "Database");
            enMap.put("suffix", "Host");
            LinkedHashMap<String, Object> messages = new LinkedHashMap<>();
            messages.put("en_US", enMap);

            DataSourceDefinitionDto def = buildDefinition(new LinkedHashMap<>(fieldMeta), messages);
            Map<String, String> result = ResourceHandler.buildConfigPathToLabelMap(def);

            assertEquals("Database - Host", result.get("testField"));
        }
    }
}
