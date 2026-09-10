package com.tapdata.tm.init;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlInitializationCompatibilityTest {

    private static final Set<String> DQL_COLLECTIONS = new LinkedHashSet<>(List.of(
            "dql_events", "dql_recovery_batches"));
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("DLQ initialization keeps TTL indexes repeatable and does not backfill historical records")
    void dqlIndexesAreRepeatableWithoutHistoricalBackfill() throws Exception {
        JsonNode patches = readPatches();
        Map<String, JsonNode> createIndexes = new LinkedHashMap<>();
        for (JsonNode patch : patches) {
            if (patch.has("createIndexes")) {
                createIndexes.put(patch.path("createIndexes").asText(), patch);
            }
        }

        assertEquals(DQL_COLLECTIONS, createIndexes.keySet());
        assertTtlIndex(createIndexes.get("dql_events"), "idx_dql_event_ttl");
        assertTtlIndex(createIndexes.get("dql_recovery_batches"), "idx_dql_batch_ttl");

        // Historical documents without ttl_at are intentionally retained; MongoDB TTL
        // indexes ignore documents that do not contain the indexed field.
        assertTrue(patches.findValuesAsText("update").stream()
                .noneMatch(DQL_COLLECTIONS::contains));
        assertTrue(patches.findValuesAsText("delete").stream()
                .noneMatch(DQL_COLLECTIONS::contains));
    }

    @Test
    @DisplayName("DLQ settings use upsert and set-on-insert semantics for safe script re-execution")
    void dqlSettingsDoNotOverwriteExistingRuntimeValues() throws Exception {
        JsonNode settingsPatch = null;
        for (JsonNode patch : readPatches()) {
            if ("Settings".equals(patch.path("update").asText())) {
                settingsPatch = patch;
                break;
            }
        }
        assertNotNull(settingsPatch);
        assertEquals(14, settingsPatch.path("updates").size());
        for (JsonNode update : settingsPatch.path("updates")) {
            assertTrue(update.path("upsert").asBoolean());
            assertFalse(update.path("u").path("$set").has("value"));
            assertTrue(update.path("u").path("$setOnInsert").has("value"));
        }
    }

    private void assertTtlIndex(JsonNode patch, String expectedName) {
        assertNotNull(patch);
        assertEquals(1, patch.path("indexes").size());
        JsonNode index = patch.path("indexes").get(0);
        assertEquals(1, index.path("key").size());
        assertEquals(1, index.path("key").path("ttl_at").asInt());
        assertEquals(1_209_600, index.path("expireAfterSeconds").asInt());
        assertEquals(expectedName, index.path("name").asText());
        assertTrue(index.path("background").asBoolean());
    }

    private JsonNode readPatches() throws Exception {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("init/idaas/4.22-7.json")) {
            assertNotNull(input);
            return objectMapper.readTree(input);
        }
    }

}
