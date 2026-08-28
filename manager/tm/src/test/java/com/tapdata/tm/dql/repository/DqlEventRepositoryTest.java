package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlRecoveryCallbackResultEnum;
import com.tapdata.tm.dql.DqlRecoveryAttemptResultEnum;
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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
        assertFalse(indexCaptor.getAllValues().stream()
                .anyMatch(index -> index.getIndexOptions().containsKey("expireAfterSeconds")));
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
    @DisplayName("query scope adds a task id filter without changing the frontend query")
    void queryScopeAddsTaskIdFilter() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.count(any(Query.class), eq(DqlEventEntity.class))).thenReturn(0L);

        repository.count(new DqlEventQueryVo(), Set.of("64f000000000000000000001"));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).count(queryCaptor.capture(), eq(DqlEventEntity.class));
        Document taskClause = (Document) ((List<?>) queryCaptor.getValue().getQueryObject().get("$and")).get(0);
        assertEquals(Set.of("64f000000000000000000001"),
                taskClause.get(DqlEventDto.FIELD_TASK_ID, Document.class).get("$in"));
    }

    @Test
    @DisplayName("empty query scope cannot match any event")
    void emptyQueryScopeCannotMatchAnyEvent() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.count(any(Query.class), eq(DqlEventEntity.class))).thenReturn(0L);

        repository.count(new DqlEventQueryVo(), Set.of());

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).count(queryCaptor.capture(), eq(DqlEventEntity.class));
        Document taskClause = (Document) ((List<?>) queryCaptor.getValue().getQueryObject().get("$and")).get(0);
        assertEquals(List.of("__NO_VISIBLE_TASK__"),
                taskClause.get(DqlEventDto.FIELD_TASK_ID, Document.class).get("$in"));
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
    @DisplayName("upsert writes capture data only on insert and derives ttl from created time")
    void upsertUsesInsertOnlyCaptureFields() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        Date created = new Date(1787580000000L);
        DqlEventDto incoming = new DqlEventDto();
        incoming.setEventId("DQL-new-000002");
        incoming.setTaskId("64f000000000000000000001");
        incoming.setTaskRecordId("record-1");
        incoming.setTableId("orders");
        incoming.setTargetNodeId("target-1");
        incoming.setTargetNodeName("postgres_sink");
        incoming.setFailedNodeId("target-1");
        incoming.setEventIdentity("sha256:same-event");
        incoming.setStatus(DqlEventStatusEnum.PENDING.name());
        incoming.setCreated(created);
        incoming.setTtlAt(new Date(created.getTime() + 60_000L));
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
        assertEquals("target-1", insert.get(DqlEventDto.FIELD_TARGET_NODE_ID));
        assertEquals("postgres_sink", insert.get(DqlEventDto.FIELD_TARGET_NODE_NAME));
        assertEquals(created, insert.get(DqlEventDto.FIELD_CREATED));
        assertEquals(created, insert.get(DqlEventDto.FIELD_TTL_AT));
        assertInstanceOf(Date.class, insert.get(DqlEventDto.FIELD_TTL_AT));
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
        assertTtlRefreshed(set);

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
        assertTtlRefreshed(set);
        assertEquals(attempt, updateCaptor.getValue().getUpdateObject()
                .get("$push", Document.class).get(DqlEventDto.FIELD_RECOVERY_ATTEMPTS));
    }

    @Test
    @DisplayName("starting recovery appends a running attempt only for the current batch lock")
    void startEventAppendsRunningAttempt() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null));
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId("A-1");
        attempt.setResult(DqlRecoveryAttemptResultEnum.RUNNING.name());

        assertTrue(repository.startEvent("DQL-1", "DQLB-1", attempt));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(queryCaptor.capture(), updateCaptor.capture(), eq(DqlEventEntity.class));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("DQL-1", query.get(DqlEventDto.FIELD_EVENT_ID));
        assertEquals(DqlEventStatusEnum.REPROCESSING.name(), query.get(DqlEventDto.FIELD_STATUS));
        assertEquals("DQLB-1", query.get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
        Document update = updateCaptor.getValue().getUpdateObject();
        assertEquals(attempt, update.get("$push", Document.class).get(DqlEventDto.FIELD_RECOVERY_ATTEMPTS));
        assertTtlRefreshed(update.get("$set", Document.class));
    }

    @Test
    @DisplayName("timing out current batch events finalizes the existing running attempt")
    void timeoutEventsFinalizesRunningAttempt() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(2L, 2L, null));
        Date now = new Date(1787580200000L);

        assertEquals(2L, repository.timeoutEvents("DQLB-1", List.of("DQL-1", "DQL-2"), now));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateMulti(queryCaptor.capture(), updateCaptor.capture(), eq(DqlEventEntity.class));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("DQLB-1", query.get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
        assertEquals(DqlEventStatusEnum.REPROCESSING.name(), query.get(DqlEventDto.FIELD_STATUS));
        assertEquals(List.of("DQL-1", "DQL-2"),
                query.get(DqlEventDto.FIELD_EVENT_ID, Document.class).get("$in"));
        Document set = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals(DqlEventStatusEnum.RECOVERY_FAILED.name(), set.get(DqlEventDto.FIELD_STATUS));
        assertNull(set.get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
        assertEquals(DqlRecoveryAttemptResultEnum.TIMEOUT.name(), set.get(DqlEventDto.FIELD_LAST_RECOVERY_RESULT));
        assertEquals(1, updateCaptor.getValue().getUpdateObject()
                .get("$inc", Document.class).get(DqlEventDto.FIELD_RECOVERY_COUNT));
        Document attemptSet = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals(DqlRecoveryAttemptResultEnum.TIMEOUT.name(),
                attemptSet.get(DqlEventDto.FIELD_RECOVERY_ATTEMPTS + ".$.result"));
        assertEquals("Recovery batch timed out",
                attemptSet.get(DqlEventDto.FIELD_RECOVERY_ATTEMPTS + ".$.message"));
        assertEquals(now, attemptSet.get(DqlEventDto.FIELD_RECOVERY_ATTEMPTS + ".$.finished_at"));
        assertFalse(updateCaptor.getValue().getUpdateObject().containsKey("$push"));
    }

    @Test
    @DisplayName("counting current batch reprocessing events uses both ownership and status")
    void countReprocessingEventsUsesBatchOwnership() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.count(any(Query.class), eq(DqlEventEntity.class))).thenReturn(1L);

        assertEquals(1L, repository.countReprocessingByBatchId("DQLB-1"));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).count(queryCaptor.capture(), eq(DqlEventEntity.class));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("DQLB-1", query.get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
        assertEquals(DqlEventStatusEnum.REPROCESSING.name(), query.get(DqlEventDto.FIELD_STATUS));
    }

    @Test
    @DisplayName("repeated event start is recognized without appending another attempt")
    void repeatedStartEventIsIdempotent() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 0L, null));
        DqlEventEntity event = new DqlEventEntity();
        event.setEventId("DQL-1");
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId("A-1");
        attempt.setBatchId("DQLB-1");
        attempt.setResult(DqlRecoveryAttemptResultEnum.RUNNING.name());
        event.setRecoveryAttempts(List.of(attempt));
        when(mongoTemplate.findOne(any(Query.class), eq(DqlEventEntity.class))).thenReturn(event);

        DqlRecoveryAttemptDto repeated = new DqlRecoveryAttemptDto();
        repeated.setAttemptId("A-1");
        repeated.setBatchId("DQLB-1");
        repeated.setResult(DqlRecoveryAttemptResultEnum.RUNNING.name());

        assertEquals(DqlRecoveryCallbackResultEnum.DUPLICATE,
                repository.startEventIdempotent("DQL-1", "DQLB-1", repeated));
        verify(mongoTemplate).findOne(any(Query.class), eq(DqlEventEntity.class));
    }

    @Test
    @DisplayName("repeated terminal event result is recognized without changing the event")
    void repeatedCompleteEventIsIdempotent() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 0L, null));
        DqlEventEntity event = new DqlEventEntity();
        event.setEventId("DQL-1");
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId("A-1");
        attempt.setBatchId("DQLB-1");
        attempt.setResult(DqlRecoveryAttemptResultEnum.SUCCESS.name());
        event.setRecoveryAttempts(List.of(attempt));
        when(mongoTemplate.findOne(any(Query.class), eq(DqlEventEntity.class))).thenReturn(event);

        assertEquals(DqlRecoveryCallbackResultEnum.DUPLICATE,
                repository.completeEventIdempotent("DQL-1", "DQLB-1", attempt));
        verify(mongoTemplate).findOne(any(Query.class), eq(DqlEventEntity.class));
    }

    @Test
    @DisplayName("terminal event result updates the existing running attempt instead of appending another record")
    void terminalEventResultUpdatesExistingRunningAttempt() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null));

        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId("A-1");
        attempt.setBatchId("DQLB-1");
        attempt.setStartedAt(new Date(1000L));
        attempt.setFinishedAt(new Date(2000L));
        attempt.setResult(DqlRecoveryAttemptResultEnum.FAILED.name());
        attempt.setMessage("payload lookup failed");
        attempt.setErrorCode("RECOVERY_FAILED");
        attempt.setErrorDetails("details");

        assertEquals(DqlRecoveryCallbackResultEnum.APPLIED,
                repository.failEventIdempotent("DQL-1", "DQLB-1", attempt));

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(any(Query.class), updateCaptor.capture(), eq(DqlEventEntity.class));
        Document update = updateCaptor.getValue().getUpdateObject();
        assertFalse(update.containsKey("$push"));
        Document set = update.get("$set", Document.class);
        assertEquals(DqlRecoveryAttemptResultEnum.FAILED.name(),
                set.get(DqlEventDto.FIELD_RECOVERY_ATTEMPTS + ".$.result"));
        assertEquals("payload lookup failed",
                set.get(DqlEventDto.FIELD_RECOVERY_ATTEMPTS + ".$.message"));
        assertEquals("details",
                set.get(DqlEventDto.FIELD_RECOVERY_ATTEMPTS + ".$." + DqlRecoveryAttemptDto.FIELD_ERROR_DETAILS));
    }

    @Test
    @DisplayName("same attempt id with a different terminal result is rejected")
    void conflictingCompleteEventIsRejected() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 0L, null));
        DqlEventEntity event = new DqlEventEntity();
        event.setEventId("DQL-1");
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId("A-1");
        attempt.setBatchId("DQLB-1");
        attempt.setResult(DqlRecoveryAttemptResultEnum.FAILED.name());
        event.setRecoveryAttempts(List.of(attempt));
        when(mongoTemplate.findOne(any(Query.class), eq(DqlEventEntity.class))).thenReturn(event);

        DqlRecoveryAttemptDto conflicting = new DqlRecoveryAttemptDto();
        conflicting.setAttemptId("A-1");
        conflicting.setBatchId("DQLB-1");
        conflicting.setResult(DqlRecoveryAttemptResultEnum.SUCCESS.name());

        assertEquals(DqlRecoveryCallbackResultEnum.CONFLICT,
                repository.completeEventIdempotent("DQL-1", "DQLB-1", conflicting));
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
        Document failSet = failUpdateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals(DqlEventStatusEnum.RECOVERY_FAILED.name(), failSet.get(DqlEventDto.FIELD_STATUS));
        assertTtlRefreshed(failSet);

        ArgumentCaptor<Query> releaseQueryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> releaseUpdateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateMulti(
                releaseQueryCaptor.capture(), releaseUpdateCaptor.capture(), eq(DqlEventEntity.class));
        assertEquals("DQLB-1",
                releaseQueryCaptor.getValue().getQueryObject().get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
        assertEquals(DqlEventStatusEnum.REPROCESSING.name(),
                releaseQueryCaptor.getValue().getQueryObject().get(DqlEventDto.FIELD_STATUS));
        Document releaseSet = releaseUpdateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertNull(releaseSet.get(DqlEventDto.FIELD_CURRENT_BATCH_ID));
        assertTtlRefreshed(releaseSet);
    }

    @Test
    @DisplayName("releasing a partially locked batch restores each event's original status")
    void releaseBatchLocksRestoresOriginalStatuses() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(DqlEventEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null));

        assertEquals(2L, repository.releaseBatchLocks("DQLB-1", Map.of(
                "DQL-1", DqlEventStatusEnum.PENDING,
                "DQL-2", DqlEventStatusEnum.RECOVERY_FAILED)));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, org.mockito.Mockito.times(2)).updateMulti(
                queryCaptor.capture(), updateCaptor.capture(), eq(DqlEventEntity.class));
        List<Document> queries = queryCaptor.getAllValues().stream()
                .map(Query::getQueryObject)
                .toList();
        List<Document> updates = updateCaptor.getAllValues().stream()
                .map(Update::getUpdateObject)
                .map(update -> update.get("$set", Document.class))
                .toList();
        assertTrue(queries.stream().allMatch(query -> "DQLB-1".equals(query.get(DqlEventDto.FIELD_CURRENT_BATCH_ID))
                && DqlEventStatusEnum.REPROCESSING.name().equals(query.get(DqlEventDto.FIELD_STATUS))));
        assertTrue(updates.stream().map(update -> update.get(DqlEventDto.FIELD_STATUS))
                .toList().containsAll(List.of(
                        DqlEventStatusEnum.PENDING.name(),
                        DqlEventStatusEnum.RECOVERY_FAILED.name())));
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
    @DisplayName("page uses twenty items when the request does not provide a positive limit")
    void pageUsesDefaultLimit() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        DqlEventQueryVo query = new DqlEventQueryVo();
        when(mongoTemplate.count(any(Query.class), eq(DqlEventEntity.class))).thenReturn(1L);
        when(mongoTemplate.find(any(Query.class), eq(DqlEventEntity.class))).thenReturn(List.of());

        repository.page(query);

        assertEquals(20, capturedFindQuery(mongoTemplate).getLimit());
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

    private void assertTtlRefreshed(Document set) {
        assertInstanceOf(Date.class, set.get(DqlEventDto.FIELD_TTL_AT));
        assertEquals(set.get(DqlEventDto.FIELD_UPDATED), set.get(DqlEventDto.FIELD_TTL_AT));
    }
}
