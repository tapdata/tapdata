package com.tapdata.tm.init.patches.daas;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class V4_25_2_WorkerApiServerUniqueTest {

    @Test
    void canonicalWorkerPrefersValidStatusBeforeRecency() {
        ObjectId staleId = new ObjectId("000000000000000000000001");
        ObjectId currentId = new ObjectId("000000000000000000000002");
        Document stale = new Document("_id", staleId)
                .append("process_id", "api-server-1")
                .append("ping_time", 200L);
        Document current = new Document("_id", currentId)
                .append("process_id", "api-server-1")
                .append("ping_time", 100L)
                .append("worker_status", new Document("status", "UP"));

        assertEquals(currentId, V4_25_2_WorkerApiServerUnique.selectCanonical(List.of(stale, current)).getObjectId("_id"));
    }
}
