package com.tapdata.tm.mcp.mongodb;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MongoOperatorPipelineTest {

    @Test
    void normalizesDateExtendedJsonBeforeSendingPipelineToMongoDB() throws Exception {
        Map<String, Object> addFields = new LinkedHashMap<>();
        addFields.put("promisedShipAtDate", Map.of("$date", "$promised_ship_at"));
        addFields.put("demoStartedAt", Map.of("$date", "2026-07-09T00:00:00Z"));
        List<Document> pipeline = new ArrayList<>(List.of(new Document("$addFields", addFields)));

        processPipeline(pipeline);

        Document normalizedAddFields = (Document) pipeline.get(0).get("$addFields");
        assertEquals(new Document("$toDate", "$promised_ship_at"), normalizedAddFields.get("promisedShipAtDate"));
        assertEquals(Date.from(Instant.parse("2026-07-09T00:00:00Z")), normalizedAddFields.get("demoStartedAt"));
    }

    @SuppressWarnings("unchecked")
    private void processPipeline(List<Document> pipeline) throws Exception {
        Method method = MongoOperator.class.getDeclaredMethod("processPipeline", List.class);
        method.setAccessible(true);
        method.invoke(new MongoOperator(null), pipeline);
    }
}
