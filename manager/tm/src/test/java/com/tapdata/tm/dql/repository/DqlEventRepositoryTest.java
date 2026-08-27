package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.DqlRecordIdentityTypeEnum;
import com.tapdata.tm.dql.entity.DqlEventEntity;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import com.tapdata.tm.task.entity.TaskEntity;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlEventRepositoryTest {

    @Test
    @DisplayName("init ensures all query indexes and a partial unique event identity index")
    void initEnsuresAllQueryIndexesWhenCollectionExists() {
        MongoTemplate mongoTemplate = mongoTemplate();

        new DqlEventRepository(mongoTemplate);

        verify(mongoTemplate, never()).createCollection("dql_events");
        IndexOperations indexOperations = mongoTemplate.indexOps("dql_events");
        ArgumentCaptor<IndexDefinition> indexCaptor = ArgumentCaptor.forClass(IndexDefinition.class);
        verify(indexOperations, times(7)).createIndex(indexCaptor.capture());
        IndexDefinition recordIdentityIndex = indexCaptor.getAllValues().stream()
                .filter(index -> "idx_task_record_identity_event_time".equals(index.getIndexOptions().getString("name")))
                .findFirst()
                .orElseThrow();
        assertFalse(recordIdentityIndex.getIndexOptions().containsKey("sparse"));
        IndexDefinition eventIdentityIndex = indexCaptor.getAllValues().stream()
                .filter(index -> "uk_task_event_identity".equals(index.getIndexOptions().getString("name")))
                .findFirst()
                .orElseThrow();
        assertEquals(true, eventIdentityIndex.getIndexOptions().get("unique"));
        assertFalse(eventIdentityIndex.getIndexOptions().containsKey("sparse"));
        Document partialFilter = eventIdentityIndex.getIndexOptions().get("partialFilterExpression", Document.class);
        assertNotNull(partialFilter);
        Document identityFilter = partialFilter.get(DqlEventDto.FIELD_EVENT_IDENTITY, Document.class);
        assertEquals("string", identityFilter.get("$type"));
        assertEquals("", identityFilter.get("$gt"));
    }

    @Test
    @DisplayName("init creates the dql event collection when it is absent")
    void initCreatesCollectionWhenAbsent() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoTemplate.collectionExists("dql_events")).thenReturn(false);
        when(mongoTemplate.indexOps("dql_events")).thenReturn(indexOperations);

        new DqlEventRepository(mongoTemplate);

        verify(mongoTemplate).createCollection("dql_events");
        verify(indexOperations, times(7)).createIndex(any(IndexDefinition.class));
    }

    @Test
    @DisplayName("capture sequence is atomically incremented on the task document")
    void nextCaptureSeqUsesAtomicTaskUpdate() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        String taskId = "64f000000000000000000001";
        TaskEntity task = new TaskEntity();
        task.setAttrs(Map.of("dqlEventSeq", 12L));
        when(mongoTemplate.findAndModify(
                any(Query.class),
                any(Update.class),
                any(FindAndModifyOptions.class),
                eq(TaskEntity.class)
        )).thenReturn(task);

        assertEquals(12L, repository.nextCaptureSeq(taskId));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).findAndModify(
                queryCaptor.capture(),
                updateCaptor.capture(),
                any(FindAndModifyOptions.class),
                eq(TaskEntity.class)
        );
        assertEquals(new ObjectId(taskId), queryCaptor.getValue().getQueryObject().get("_id"));
        assertEquals(1, updateCaptor.getValue().getUpdateObject()
                .get("$inc", Document.class).get("attrs.dqlEventSeq"));
        assertThrows(BizException.class, () -> repository.nextCaptureSeq("not-an-object-id"));
    }

    @Test
    @DisplayName("upsert writes capture data only on insert so concurrent duplicates cannot replace the event")
    void upsertUsesInsertOnlyCaptureFields() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        Date created = new Date(1787580000000L);
        DqlEventDto incoming = new DqlEventDto();
        incoming.setEventId("DQL-new-000002");
        incoming.setTaskId("64f000000000000000000001");
        incoming.setTaskRecordId("record-1");
        incoming.setTableId("orders");
        incoming.setFailedNodeId("target-1");
        incoming.setEventIdentity("sha256:same-event");
        incoming.setStatus(DqlEventStatusEnum.PENDING.name());
        incoming.setCreated(created);
        DqlEventEntity existing = new DqlEventEntity();
        existing.setEventId("DQL-existing-000001");
        existing.setTaskId(incoming.getTaskId());
        when(mongoTemplate.findAndModify(
                any(Query.class),
                any(Update.class),
                any(FindAndModifyOptions.class),
                eq(DqlEventEntity.class)
        )).thenReturn(existing);

        DqlEventDto result = repository.upsert(incoming);

        assertEquals("DQL-existing-000001", result.getEventId());
        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).findAndModify(
                queryCaptor.capture(),
                updateCaptor.capture(),
                any(FindAndModifyOptions.class),
                eq(DqlEventEntity.class)
        );
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals(incoming.getTaskId(), query.get(DqlEventDto.FIELD_TASK_ID));
        assertEquals(incoming.getEventIdentity(), query.get(DqlEventDto.FIELD_EVENT_IDENTITY));
        Document update = updateCaptor.getValue().getUpdateObject();
        assertNull(update.get("$set"));
        Document insert = update.get("$setOnInsert", Document.class);
        assertEquals("DQL-new-000002", insert.get(DqlEventDto.FIELD_EVENT_ID));
        assertEquals(created, insert.get(DqlEventDto.FIELD_CREATED));
        assertEquals(created, insert.get(DqlEventDto.FIELD_TTL_AT));
    }

    @Test
    @DisplayName("locking recoverable events refreshes ttl in the same update")
    void lockEventsRefreshesTtl() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null));

        assertEquals(1L, repository.lockEvents(List.of("DQL-1"), "DQLB-1"));

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateMulti(any(Query.class), updateCaptor.capture(), eq(DqlEventEntity.class));
        Document set = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals(DqlEventStatusEnum.REPROCESSING.name(), set.get("status"));
        assertEquals(set.get("updated"), set.get("ttl_at"));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).updateMulti(queryCaptor.capture(), any(Update.class), eq(DqlEventEntity.class));
        assertEquals(List.of(DqlEventStatusEnum.PENDING.name(), DqlEventStatusEnum.RECOVERY_FAILED.name()),
                queryCaptor.getValue().getQueryObject().get("status", Document.class).get("$in"));
        assertEquals(null, queryCaptor.getValue().getQueryObject().get("current_batch_id"));
    }

    @Test
    @DisplayName("finishing recovery refreshes event ttl")
    void completeEventRefreshesTtl() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null));
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setFinishedAt(new Date());

        assertEquals(true, repository.completeEvent("DQL-1", "DQLB-1", attempt));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(queryCaptor.capture(), updateCaptor.capture(), eq(DqlEventEntity.class));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("DQL-1", query.get(DqlEventDto.FIELD_EVENT_ID));
        assertEquals(DqlEventStatusEnum.REPROCESSING.name(), query.get(DqlEventDto.FIELD_STATUS));
        assertEquals("DQLB-1", query.get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
        Document set = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals(DqlEventStatusEnum.RECOVERED.name(), set.get("status"));
        assertEquals(set.get("updated"), set.get("ttl_at"));
        assertEquals(attempt, updateCaptor.getValue().getUpdateObject()
                .get("$push", Document.class).get(DqlEventDto.FIELD_RECOVERY_ATTEMPTS));
    }

    @Test
    @DisplayName("failed recovery and batch lock release use guarded state transitions")
    void failureAndReleaseUseGuardedStateTransitions() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null));
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(2L, 2L, null));
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setFinishedAt(new Date());
        attempt.setResult("FAILED");

        assertEquals(true, repository.failEvent("DQL-1", "DQLB-1", attempt));
        assertEquals(2L, repository.releaseBatchLocks("DQLB-1", DqlEventStatusEnum.RECOVERY_FAILED));

        ArgumentCaptor<Query> failQueryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> failUpdateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(
                failQueryCaptor.capture(), failUpdateCaptor.capture(), eq(DqlEventEntity.class));
        assertEquals(DqlEventStatusEnum.REPROCESSING.name(),
                failQueryCaptor.getValue().getQueryObject().get(DqlEventDto.FIELD_STATUS));
        assertEquals("DQLB-1",
                failQueryCaptor.getValue().getQueryObject().get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
        assertEquals(DqlEventStatusEnum.RECOVERY_FAILED.name(), failUpdateCaptor.getValue()
                .getUpdateObject().get("$set", Document.class).get(DqlEventDto.FIELD_STATUS));

        ArgumentCaptor<Query> releaseQueryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> releaseUpdateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateMulti(
                releaseQueryCaptor.capture(), releaseUpdateCaptor.capture(), eq(DqlEventEntity.class));
        assertEquals("DQLB-1",
                releaseQueryCaptor.getValue().getQueryObject().get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
        assertEquals(DqlEventStatusEnum.REPROCESSING.name(),
                releaseQueryCaptor.getValue().getQueryObject().get(DqlEventDto.FIELD_STATUS));
        assertNull(releaseUpdateCaptor.getValue().getUpdateObject()
                .get("$set", Document.class).get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
    }

    @Test
    @DisplayName("page maps frontend recovery count sort field to mongo field")
    void pageMapsRecoveryCountSortField() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        DqlEventQueryVo query = new DqlEventQueryVo();
        query.setOrder("-recoveryCount");
        query.setLimit(20);
        when(mongoTemplate.count(any(Query.class), eq(DqlEventEntity.class))).thenReturn(1L);
        when(mongoTemplate.find(any(Query.class), eq(DqlEventEntity.class))).thenReturn(List.of());

        repository.page(query);

        Document sort = capturedFindQuery(mongoTemplate).getSortObject();
        assertEquals(-1, sort.get("recovery_count"));
        assertFalse(sort.containsKey("recoveryCount"));
    }

    @Test
    @DisplayName("page maps frontend last recovery time sort field to mongo field")
    void pageMapsLastRecoveryTimeSortField() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        DqlEventQueryVo query = new DqlEventQueryVo();
        query.setOrder("lastRecoveryTime");
        query.setLimit(20);
        when(mongoTemplate.count(any(Query.class), eq(DqlEventEntity.class))).thenReturn(1L);
        when(mongoTemplate.find(any(Query.class), eq(DqlEventEntity.class))).thenReturn(List.of());

        repository.page(query);

        Document sort = capturedFindQuery(mongoTemplate).getSortObject();
        assertEquals(1, sort.get("last_recovery_time"));
        assertFalse(sort.containsKey("lastRecoveryTime"));
    }

    @Test
    @DisplayName("page uses ten items when the request does not provide a positive limit")
    void pageUsesDefaultLimit() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        DqlEventQueryVo query = new DqlEventQueryVo();
        when(mongoTemplate.count(any(Query.class), eq(DqlEventEntity.class))).thenReturn(1L);
        when(mongoTemplate.find(any(Query.class), eq(DqlEventEntity.class))).thenReturn(List.of());

        repository.page(query);

        assertEquals(10, capturedFindQuery(mongoTemplate).getLimit());
    }

    @Test
    @DisplayName("mark later success flags the latest unresolved DLQ event for the same record")
    void markLaterSuccessFlagsLatestUnresolvedDqlEvent() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        String warning = "该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作";
        DqlRecordSuccessReportVo report = new DqlRecordSuccessReportVo();
        report.setTaskRecordId("record-1");
        report.setTableId("orders");
        report.setRecordIdentity("key:orders:id=1001");
        report.setRecordIdentityType(DqlRecordIdentityTypeEnum.PRIMARY_KEY.name());
        report.setDmlType("U");
        report.setEventTime(1787580100000L);
        report.setCaptureSeq(12L);
        report.setSuccessAt(1787580102300L);
        DqlEventEntity marked = new DqlEventEntity();
        marked.setEventId("DQL-64f000-000001");
        marked.setOverwriteRisk(true);
        marked.setOverwriteRiskMessage(warning);
        when(mongoTemplate.findAndModify(
                any(Query.class),
                any(Update.class),
                any(FindAndModifyOptions.class),
                eq(DqlEventEntity.class)
        )).thenReturn(marked);

        assertEquals("DQL-64f000-000001",
                repository.markLaterSuccess("64f000000000000000000001", report, warning).getEventId());

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).findAndModify(
                queryCaptor.capture(),
                updateCaptor.capture(),
                any(FindAndModifyOptions.class),
                eq(DqlEventEntity.class)
        );
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("64f000000000000000000001", query.get("task_id"));
        assertEquals("record-1", query.get("task_record_id"));
        assertEquals("orders", query.get("table_id"));
        assertEquals("key:orders:id=1001", query.get("record_identity"));
        Document sort = queryCaptor.getValue().getSortObject();
        assertEquals(-1, sort.get("event_time"));
        assertEquals(-1, sort.get("capture_seq"));
        Document set = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertNotNull(set);
        assertEquals(true, set.get("overwrite_risk"));
        assertEquals(warning, set.get("overwrite_risk_message"));
        assertEquals(new Date(1787580102300L), set.get("later_success_at"));
        assertEquals(new Date(1787580100000L), set.get("later_success_event_time"));
        assertEquals(12L, set.get("later_success_capture_seq"));
        assertEquals("U", set.get("later_success_dml_type"));
    }

    private MongoTemplate mongoTemplate() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoTemplate.collectionExists("dql_events")).thenReturn(true);
        when(mongoTemplate.indexOps("dql_events")).thenReturn(indexOperations);
        return mongoTemplate;
    }

    private Query capturedFindQuery(MongoTemplate mongoTemplate) {
        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).find(captor.capture(), eq(DqlEventEntity.class));
        return captor.getValue();
    }
}
