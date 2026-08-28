package com.tapdata.tm.init;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlAlarmInitializationPatchV8Test {

    private static final Set<String> DQL_ALARM_KEYS = Set.of(
            "TASK_DQL_EVENT", "TASK_DQL_SAVE_FAILED", "TASK_DQL_RECOVERY_FAILED", "TASK_DQL_STORM_GUARD");
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("4.22-8 initializes DQL alarm settings for system and email channels")
    void initializesDqlAlarmSettingsForBothChannels() throws Exception {
        JsonNode settings = findSettingsAlarmPatch(readPatches());
        assertNotNull(settings);
        for (String key : DQL_ALARM_KEYS) {
            JsonNode defaultUpdate = findUpdate(settings, key, true);
            assertNotNull(defaultUpdate, key);
            JsonNode insert = defaultUpdate.path("u").path("$setOnInsert");
            assertEquals(List.of("SYSTEM", "EMAIL"), objectMapper.convertValue(insert.path("notify"), List.class));
            assertTrue(insert.path("open").asBoolean());
            assertTrue(insert.path("emailAlarmTitle").asText().contains("{taskName}"));
            assertTrue(insert.path("emailAlarmContent").asText().contains("{pageUrl}"));
            assertTrue(insert.path("variables").size() > 0);
        }
    }

    @Test
    @DisplayName("4.22-8 repairs only the legacy 4.22-7 DQL defaults")
    void repairsLegacyDefaultsWithoutReplacingCustomizedSettings() throws Exception {
        JsonNode settings = findSettingsAlarmPatch(readPatches());
        assertNotNull(settings);
        for (String key : DQL_ALARM_KEYS) {
            JsonNode defaultUpdate = findUpdate(settings, key, true);
            JsonNode repairUpdate = findUpdate(settings, key, false);
            assertTrue(defaultUpdate.path("u").has("$setOnInsert"));
            assertTrue(repairUpdate.path("q").path("notify").has(0));
            assertEquals("SYSTEM", repairUpdate.path("q").path("notify").get(0).asText());
            assertEquals(List.of("SYSTEM", "EMAIL"),
                    objectMapper.convertValue(repairUpdate.path("u").path("$set").path("notify"), List.class));
            assertTrue(repairUpdate.path("u").path("$set").path("emailAlarmContent").asText().contains("{pageUrl}"));
            assertEquals(defaultUpdate.path("u").path("$setOnInsert").path("variables").size(),
                    repairUpdate.path("u").path("$set").path("variables").size());
        }

        JsonNode taskPatch = findPatch(readPatches(), "Task");
        assertNotNull(taskPatch);
        JsonNode taskUpdate = taskPatch.path("updates").get(0);
        assertEquals(List.of("SYSTEM", "EMAIL"),
                objectMapper.convertValue(taskUpdate.path("u").path("$set").path("alarmSettings.$[dql].notify"), List.class));
        assertEquals(List.of("SYSTEM"),
                objectMapper.convertValue(taskUpdate.path("q").path("alarmSettings").path("$elemMatch").path("notify"), List.class));
        assertEquals(List.of("SYSTEM"),
                objectMapper.convertValue(taskUpdate.path("arrayFilters").get(0).path("dql.notify"), List.class));
    }

    private JsonNode readPatches() throws Exception {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("init/idaas/4.22-8.json")) {
            assertNotNull(input);
            return objectMapper.readTree(input);
        }
    }

    private JsonNode findUpdate(JsonNode patch, String key, boolean defaultUpdate) {
        for (JsonNode update : patch.path("updates")) {
            if (key.equals(update.path("q").path("key").asText())
                    && (defaultUpdate == update.path("u").has("$setOnInsert"))) {
                return update;
            }
        }
        return null;
    }

    private JsonNode findSettingsAlarmPatch(JsonNode patches) {
        return findPatch(patches, "Settings_Alarm");
    }

    private JsonNode findPatch(JsonNode patches, String collection) {
        for (JsonNode patch : patches) {
            if (collection.equals(patch.path("update").asText())) {
                return patch;
            }
        }
        return null;
    }
}
