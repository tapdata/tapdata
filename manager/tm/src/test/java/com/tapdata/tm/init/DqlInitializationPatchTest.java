package com.tapdata.tm.init;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlInitializationPatchTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("DAAS 4.22-7 upgrade exposes the DLQ exception event page to administrators")
    void upgradeInstallsDqlExceptionEventPermissionAndRoleMapping() throws Exception {
        JsonNode patches = readPatches();

        JsonNode permissionPatch = findPatch(patches, "Permission");
        assertNotNull(permissionPatch);
        JsonNode permission = findUpdate(permissionPatch, "v2_exception_events");
        assertNotNull(permission);
        assertEquals("v2_advanced_features", permission.path("u").path("$set").path("parentId").asText());
        assertEquals("异常事件", permission.path("u").path("$set").path("description").asText());
        assertEquals("read", permission.path("u").path("$set").path("type").asText());
        assertEquals("/exception-events", permission.path("u").path("$set").path("resources").get(0).path("path").asText());

        JsonNode roleMappingPatch = findPatch(patches, "RoleMapping");
        assertNotNull(roleMappingPatch);
        JsonNode roleMapping = findUpdate(roleMappingPatch, "v2_exception_events");
        assertNotNull(roleMapping);
        assertEquals("PERMISSION", roleMapping.path("q").path("principalType").asText());
        assertEquals("5b9a0a383fcba02649524bf1", roleMapping.path("q").path("roleId").path("$oid").asText());
        assertFalse(roleMapping.path("u").path("$set").path("self_only").asBoolean());

        JsonNode alarmPatch = findPatch(patches, "Settings_Alarm");
        assertNotNull(alarmPatch);
        for (String key : List.of("TASK_DQL_EVENT", "TASK_DQL_SAVE_FAILED", "TASK_DQL_RECOVERY_FAILED", "TASK_DQL_STORM_GUARD")) {
            JsonNode alarm = findUpdate(alarmPatch, key);
            assertNotNull(alarm);
            assertTrue(alarm.path("upsert").asBoolean());
            assertEquals("TASK", alarm.path("u").path("$set").path("type").asText());
            assertTrue(alarm.path("u").path("$set").path("open").asBoolean());
            assertEquals("SYSTEM", alarm.path("u").path("$set").path("notify").get(0).asText());
        }

        JsonNode settingsPatch = findPatch(patches, "Settings");
        assertNotNull(settingsPatch);
        Map<String, Object> expectedDefaults = new LinkedHashMap<>();
        expectedDefaults.put("dql.event.enabled", true);
        expectedDefaults.put("dql.event.errorDetails.maxLength", 4000);
        expectedDefaults.put("dql.event.payload.maxBytes", 1048576);
        expectedDefaults.put("dql.event.preview.fieldMaxLength", 512);
        expectedDefaults.put("dql.event.preview.maxDepth", 4);
        expectedDefaults.put("dql.event.preview.maxItems", 50);
        expectedDefaults.put("dql.recovery.batch.maxSize", 200);
        expectedDefaults.put("dql.recovery.eventTimeoutSeconds", 60);
        expectedDefaults.put("dql.recovery.batchTimeoutSeconds", 1800);
        expectedDefaults.put("dql.recovery.continueOnEventFailure", true);
        expectedDefaults.put("dql.unknown.guard.windowSeconds", 60);
        expectedDefaults.put("dql.unknown.guard.maxEvents", 20);
        expectedDefaults.put("dql.unknown.guard.maxBatchRatio", 0.2d);
        expectedDefaults.put("dql.unknown.guard.decision", "TASK_RETRY");
        assertEquals(expectedDefaults.size(), settingsPatch.path("updates").size());
        expectedDefaults.forEach((key, value) -> {
            JsonNode update = findUpdate(settingsPatch, key);
            assertNotNull(update);
            assertTrue(update.path("upsert").asBoolean());
            JsonNode set = update.path("u").path("$set");
            assertEquals("System", set.path("category").asText());
            assertFalse(set.path("user_visible").asBoolean());
            assertFalse(set.path("hot_reloading").asBoolean());
            assertEquals(value.toString(), set.path("default_value").asText());
            assertEquals(value.toString(), update.path("u").path("$setOnInsert").path("value").asText());
        });
    }

    private JsonNode readPatches() throws Exception {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("init/idaas/4.22-7.json")) {
            assertNotNull(input);
            return objectMapper.readTree(input);
        }
    }

    private JsonNode findPatch(JsonNode patches, String command) {
        for (JsonNode patch : patches) {
            if (command.equals(patch.path("update").asText())) {
                return patch;
            }
        }
        return null;
    }

    private JsonNode findUpdate(JsonNode patch, String name) {
        for (JsonNode update : patch.path("updates")) {
            JsonNode query = update.path("q");
            if (name.equals(query.path("name").asText())
                    || name.equals(query.path("principalId").asText())
                    || name.equals(query.path("key").asText())) {
                return update;
            }
        }
        return null;
    }
}
