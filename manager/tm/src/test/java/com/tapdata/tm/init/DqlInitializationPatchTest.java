package com.tapdata.tm.init;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DqlInitializationPatchTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("DAAS 4.22-7 upgrade exposes the DQL exception event page to administrators")
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
            if (name.equals(query.path("name").asText()) || name.equals(query.path("principalId").asText())) {
                return update;
            }
        }
        return null;
    }
}
