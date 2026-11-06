package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class V4_25_3_AuditAndRecoveryTest {

    @Test
    @DisplayName("历史补丁：仅重写含凭证的记录，安全记录保持原样")
    void should_mask_only_sensitive_documents() {
        MongoTemplate mongo = mock(MongoTemplate.class);
        Document sensitive = new Document("_id", new ObjectId())
                .append("parameter1", "MCP POST /mcp?accessCode=synthetic-code");
        Document safe = new Document("_id", new ObjectId()).append("parameter1", "Created connection orders");
        List<Document> batch = new ArrayList<>(List.of(sensitive, safe));
        when(mongo.find(any(Query.class), eq(Document.class), eq("UserLogs")))
                .thenReturn(batch, Collections.emptyList());
        UpdateResult result = mock(UpdateResult.class);
        when(result.getMatchedCount()).thenReturn(1L);
        when(result.getModifiedCount()).thenReturn(1L);
        when(mongo.updateFirst(any(Query.class), any(Update.class), anyString())).thenReturn(result);
        MongoCollection<Document> alarms = alarmCollection();
        when(mongo.getCollection("AlarmInfo")).thenReturn(alarms);

        V4_25_3_AuditAndRecovery.migrate(mongo);

        verify(mongo, times(1)).updateFirst(any(Query.class), any(Update.class), eq("UserLogs"));
    }

    @Test
    @DisplayName("历史补丁：重复执行不重复修改已脱敏记录")
    void should_be_idempotent() {
        MongoTemplate mongo = mock(MongoTemplate.class);
        Document masked = new Document("_id", new ObjectId())
                .append("parameter1", "MCP POST /mcp?accessCode=***");
        when(mongo.find(any(Query.class), eq(Document.class), eq("UserLogs")))
                .thenReturn(new ArrayList<>(List.of(masked)), Collections.emptyList());
        MongoCollection<Document> alarms = alarmCollection();
        when(mongo.getCollection("AlarmInfo")).thenReturn(alarms);

        V4_25_3_AuditAndRecovery.migrate(mongo);

        verify(mongo, never()).updateFirst(any(Query.class), any(Update.class), anyString());
    }

    @Test
    @DisplayName("历史补丁：仅修正错误恢复状态，不动已关闭事件与通知时间")
    @SuppressWarnings("unchecked")
    void should_fix_only_broken_recovery_status() {
        MongoTemplate mongo = mock(MongoTemplate.class);
        when(mongo.find(any(Query.class), eq(Document.class), eq("UserLogs")))
                .thenReturn(Collections.emptyList());
        MongoCollection<Document> alarms = alarmCollection();
        when(mongo.getCollection("AlarmInfo")).thenReturn(alarms);

        V4_25_3_AuditAndRecovery.migrate(mongo);

        org.mockito.ArgumentCaptor<Document> filter = org.mockito.ArgumentCaptor.forClass(Document.class);
        verify(alarms).updateMany(filter.capture(), any(List.class));
        assertEquals("ENGINE_ONLINE", filter.getValue().get("metric"));
        assertEquals("RECOVERY", filter.getValue().get("level"));
        assertEquals("ING", filter.getValue().get("status"));
        assertTrue(!filter.getValue().containsKey("lastNotifyTime"));
    }

    @SuppressWarnings("unchecked")
    private MongoCollection<Document> alarmCollection() {
        MongoCollection<Document> collection = mock(MongoCollection.class);
        UpdateResult result = mock(UpdateResult.class);
        when(result.getModifiedCount()).thenReturn(0L);
        when(collection.updateMany(any(Document.class), any(List.class))).thenReturn(result);
        return collection;
    }
}
