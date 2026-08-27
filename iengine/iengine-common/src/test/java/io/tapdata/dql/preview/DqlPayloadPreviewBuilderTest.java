package io.tapdata.dql.preview;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlPayloadPreviewBuilderTest {

    @Test
    @DisplayName("insert, update and delete previews contain only their applicable record images")
    void buildsDmlSpecificPreview() {
        DqlPayloadPreviewBuilder builder = new DqlPayloadPreviewBuilder();
        Map<String, Object> before = Map.of("id", 1, "status", "new");
        Map<String, Object> after = Map.of("id", 1, "status", "paid");

        Map<String, Object> insertPreview = builder.build(
                TapInsertRecordEvent.create().table("orders").after(after), Map.of("id", 1)).getPayloadPreview();
        Map<String, Object> updatePreview = builder.build(
                TapUpdateRecordEvent.create().table("orders").before(before).after(after), Map.of("id", 1)).getPayloadPreview();
        Map<String, Object> deletePreview = builder.build(
                TapDeleteRecordEvent.create().table("orders").before(before), Map.of("id", 1)).getPayloadPreview();

        assertEquals(Map.of("id", 1), insertPreview.get("key"));
        assertEquals(after, insertPreview.get("after"));
        assertFalse(insertPreview.containsKey("before"));
        assertEquals(before, updatePreview.get("before"));
        assertEquals(after, updatePreview.get("after"));
        assertEquals(before, deletePreview.get("before"));
        assertFalse(deletePreview.containsKey("after"));
    }

    @Test
    @DisplayName("sensitive values are masked without mutating the source event")
    void masksSensitiveFields() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("token", "token-secret");
        nested.put("safe", "visible");
        Map<String, Object> after = new LinkedHashMap<>();
        after.put("password", "password-secret");
        after.put("nested", nested);
        TapInsertRecordEvent event = TapInsertRecordEvent.create().table("orders").after(after);

        DqlPayloadPreview preview = new DqlPayloadPreviewBuilder().build(event, Map.of("id", 1));
        Map<?, ?> sanitizedAfter = (Map<?, ?>) preview.getPayloadPreview().get("after");

        assertEquals("******", sanitizedAfter.get("password"));
        assertEquals("******", ((Map<?, ?>) sanitizedAfter.get("nested")).get("token"));
        assertEquals("visible", ((Map<?, ?>) sanitizedAfter.get("nested")).get("safe"));
        assertEquals("password-secret", after.get("password"));
        assertEquals("token-secret", nested.get("token"));
        assertTrue(((List<?>) preview.getPayloadPreview().get("maskedFields")).contains("password"));
        assertTrue(((List<?>) preview.getPayloadPreview().get("maskedFields")).contains("token"));
        assertNotSame(after, sanitizedAfter);
    }

    @Test
    @DisplayName("field length, depth and item limits are reflected in the preview")
    void recordsPreviewTruncation() {
        Map<String, Object> deep = new LinkedHashMap<>();
        deep.put("level2", Map.of("level3", "hidden"));
        List<Integer> manyItems = new ArrayList<>();
        for (int index = 0; index < 4; index++) {
            manyItems.add(index);
        }
        Map<String, Object> after = new LinkedHashMap<>();
        after.put("longText", "123456789");
        after.put("deep", deep);
        after.put("items", manyItems);

        DqlPayloadPreview preview = new DqlPayloadPreviewBuilder(5, 1, 3)
                .build(TapInsertRecordEvent.create().table("orders").after(after), Map.of());
        Map<?, ?> sanitizedAfter = (Map<?, ?>) preview.getPayloadPreview().get("after");

        assertEquals(5, ((String) sanitizedAfter.get("longText")).length());
        assertEquals("...", ((Map<?, ?>) sanitizedAfter.get("deep")).get("level2"));
        assertEquals(List.of(0, 1, 2), sanitizedAfter.get("items"));
        assertTrue(preview.isTruncated());
        assertTrue(((List<?>) preview.getPayloadPreview().get("truncatedFields")).contains("longText"));
        assertTrue(((List<?>) preview.getPayloadPreview().get("truncatedFields")).contains("level2"));
        assertTrue(((List<?>) preview.getPayloadPreview().get("truncatedFields")).contains("items"));
    }
}
