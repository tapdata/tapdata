package com.tapdata.tm.commons.inspect;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class CustomQueryToleranceContractTest {
    private CustomQueryConfig.Snapshot snapshot() {
        var snapshot = new CustomQueryConfig.Snapshot();
        var column = new CustomQueryConfig.Column(); column.setName("v");
        snapshot.setColumns(List.of(column));
        return snapshot;
    }
    private CustomQueryConfig config(String json) throws Exception {
        return new ObjectMapper().readValue("{\"mappings\":[{\"source\":\"v\",\"target\":\"v\"," + json + "}]}", CustomQueryConfig.class);
    }
    @Test void existingConfigAndStringThresholdsValidateAndRoundTrip() throws Exception {
        var legacy = config("\"role\":\"compare\"");
        legacy.validate(snapshot(), snapshot());
        assertNull(legacy.getMappings().get(0).getRelativeTolerance());
        var configured = config("\"role\":\"compare\",\"absoluteTolerance\":\"0.000000000000000001234567890123456789\",\"relativeTolerance\":\"1e-8\"");
        configured.validate(snapshot(), snapshot());
        var mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(configured);
        assertTrue(mapper.readTree(json).get("mappings").get(0).get("absoluteTolerance").isTextual());
        assertEquals(configured, mapper.readValue(json, CustomQueryConfig.class));
        assertEquals(new BigDecimal("1e-8"), configured.getMappings().get(0).getRelativeTolerance());
    }
    @Test void invalidToleranceFailsSaveValidationAndCannotBeAssignedToKeyOrIgnore() throws Exception {
        for (String value : List.of("-1", "1e101", "1e-101", "1".repeat(101))) {
            var c = config("\"role\":\"compare\",\"relativeTolerance\":\"" + value + "\"");
            assertThrows(IllegalArgumentException.class, () -> c.validate(snapshot(), snapshot()));
        }
        for (String role : List.of("key", "ignore")) {
            var c = config("\"role\":\"" + role + "\",\"absoluteTolerance\":\"0\"");
            assertThrows(IllegalArgumentException.class, () -> c.validate(snapshot(), snapshot()));
        }
        for (String value : List.of("NaN", "Infinity", "abc")) {
            assertThrows(com.fasterxml.jackson.core.JsonProcessingException.class,
                    () -> config("\"role\":\"compare\",\"absoluteTolerance\":\"" + value + "\""));
        }
    }
}
