package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.dql.DqlRecoveryBatchStatusEnum;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.entity.DqlRecoveryBatchEntity;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlRecoveryBatchRepositoryTest {

    @Test
    @DisplayName("init ensures batch query indexes when collection already exists")
    void initEnsuresQueryIndexesWhenCollectionExists() {
        MongoTemplate mongoTemplate = mongoTemplate();

        new DqlRecoveryBatchRepository(mongoTemplate);

        verify(mongoTemplate, never()).createCollection("dql_recovery_batches");
        IndexOperations indexOperations = mongoTemplate.indexOps("dql_recovery_batches");
        ArgumentCaptor<IndexDefinition> indexCaptor = ArgumentCaptor.forClass(IndexDefinition.class);
        verify(indexOperations, times(3)).createIndex(indexCaptor.capture());
        assertEquals(true, indexCaptor.getAllValues().stream()
                .anyMatch(index -> "idx_task_created".equals(index.getIndexOptions().getString("name"))));
        assertFalse(indexCaptor.getAllValues().stream()
                .anyMatch(index -> index.getIndexOptions().containsKey("expireAfterSeconds")));
    }

    @Test
    @DisplayName("create initializes batch ttl from created time")
    void createInitializesTtlFromCreated() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);
        when(mongoTemplate.save(any(DqlRecoveryBatchEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId("DQLB-1");
        Date created = new Date(1787580000000L);
        batch.setCreated(created);
        batch.setTtlAt(new Date(created.getTime() + 60_000L));

        DqlRecoveryBatchDto saved = repository.create(batch);

        assertEquals(created, saved.getCreated());
        assertEquals(created, saved.getTtlAt());
        assertInstanceOf(Date.class, saved.getTtlAt());
    }

    @Test
    @DisplayName("create defaults status and counters")
    void createDefaultsStatusAndCounters() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);
        when(mongoTemplate.save(any(DqlRecoveryBatchEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        DqlRecoveryBatchDto saved = repository.create(new DqlRecoveryBatchDto());

        assertEquals(DqlRecoveryBatchStatusEnum.CREATED.name(), saved.getStatus());
        assertEquals(0, saved.getSelectedCount());
        assertEquals(0, saved.getSuccessCount());
        assertEquals(0, saved.getFailedCount());
        assertEquals(0, saved.getSkippedCount());
    }

    @Test
    @DisplayName("finishing batch refreshes ttl in the same update")
    void finishRefreshesTtl() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(DqlRecoveryBatchEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null));

        repository.finish("DQLB-1", DqlRecoveryBatchStatusEnum.SUCCESS, "done");

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(any(Query.class), updateCaptor.capture(), eq(DqlRecoveryBatchEntity.class));
        Document set = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals(DqlRecoveryBatchStatusEnum.SUCCESS.name(), set.get("status"));
        assertEquals(set.get("finished_at"), set.get("updated"));
        assertTtlRefreshed(set);
    }

    @Test
    @DisplayName("status update only targets an active created batch")
    void statusUpdateOnlyTargetsCreatedBatch() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);

        repository.updateStatus("DQLB-1", DqlRecoveryBatchStatusEnum.DISPATCHED, null);

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(
                queryCaptor.capture(), updateCaptor.capture(), eq(DqlRecoveryBatchEntity.class));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("DQLB-1", query.get("batch_id"));
        assertEquals(List.of(DqlRecoveryBatchStatusEnum.CREATED.name()),
                query.get("status", Document.class).get("$in"));
        Document set = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals(DqlRecoveryBatchStatusEnum.DISPATCHED.name(), set.get("status"));
        assertTtlRefreshed(set);
    }

    @Test
    @DisplayName("marking a dispatched batch running refreshes ttl with its start time")
    void markRunningRefreshesTtl() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);

        repository.markRunning("DQLB-1");

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(
                queryCaptor.capture(), updateCaptor.capture(), eq(DqlRecoveryBatchEntity.class));
        assertEquals(List.of(DqlRecoveryBatchStatusEnum.DISPATCHED.name()),
                queryCaptor.getValue().getQueryObject().get("status", Document.class).get("$in"));
        Document set = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals(DqlRecoveryBatchStatusEnum.RUNNING.name(), set.get("status"));
        assertEquals(set.get("started_at"), set.get("updated"));
        assertTtlRefreshed(set);
    }

    @Test
    @DisplayName("status update rejects targets other than dispatched")
    void statusUpdateRejectsNonDispatchedTarget() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);

        assertThrows(IllegalArgumentException.class,
                () -> repository.updateStatus("DQLB-1", DqlRecoveryBatchStatusEnum.RUNNING, null));

        verify(mongoTemplate, never()).updateFirst(any(Query.class), any(Update.class), eq(DqlRecoveryBatchEntity.class));
    }

    @Test
    @DisplayName("finish rejects non-terminal target status")
    void finishRejectsNonTerminalTargetStatus() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);

        assertThrows(IllegalArgumentException.class,
                () -> repository.finish("DQLB-1", DqlRecoveryBatchStatusEnum.RUNNING, null));

        verify(mongoTemplate, never()).updateFirst(any(Query.class), any(Update.class), eq(DqlRecoveryBatchEntity.class));
    }

    @Test
    @DisplayName("finish uses state-specific source guards for every terminal status")
    void finishUsesStateSpecificSourceGuards() {
        assertFinishSourceStatuses(
                DqlRecoveryBatchStatusEnum.SUCCESS,
                DqlRecoveryBatchStatusEnum.RUNNING);
        assertFinishSourceStatuses(
                DqlRecoveryBatchStatusEnum.PARTIAL_FAILED,
                DqlRecoveryBatchStatusEnum.DISPATCHED,
                DqlRecoveryBatchStatusEnum.RUNNING);
        assertFinishSourceStatuses(
                DqlRecoveryBatchStatusEnum.FAILED,
                DqlRecoveryBatchStatusEnum.CREATED,
                DqlRecoveryBatchStatusEnum.DISPATCHED,
                DqlRecoveryBatchStatusEnum.RUNNING);
        assertFinishSourceStatuses(
                DqlRecoveryBatchStatusEnum.CANCELED,
                DqlRecoveryBatchStatusEnum.CREATED);
    }

    private void assertFinishSourceStatuses(DqlRecoveryBatchStatusEnum targetStatus,
                                            DqlRecoveryBatchStatusEnum... expectedSourceStatuses) {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);

        repository.finish("DQLB-1", targetStatus, null);

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).updateFirst(queryCaptor.capture(), any(Update.class), eq(DqlRecoveryBatchEntity.class));
        assertEquals(List.of(expectedSourceStatuses).stream().map(Enum::name).toList(),
                queryCaptor.getValue().getQueryObject().get("status", Document.class).get("$in"));
    }

    @Test
    @DisplayName("active batch query filters task and non-terminal states")
    void activeBatchQueryFiltersTaskAndNonTerminalStates() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);
        when(mongoTemplate.findOne(any(Query.class), eq(DqlRecoveryBatchEntity.class))).thenReturn(null);

        repository.findActiveByTaskId("TASK-1");

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).findOne(queryCaptor.capture(), eq(DqlRecoveryBatchEntity.class));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("TASK-1", query.get("task_id"));
        assertEquals(List.of(
                DqlRecoveryBatchStatusEnum.CREATED.name(),
                DqlRecoveryBatchStatusEnum.DISPATCHED.name(),
                DqlRecoveryBatchStatusEnum.RUNNING.name()), query.get("status", Document.class).get("$in"));
        assertEquals(-1, queryCaptor.getValue().getSortObject().get("created"));
    }

    @Test
    @DisplayName("counter update only targets active batch")
    void counterUpdateOnlyTargetsActiveBatch() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(DqlRecoveryBatchEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null));

        repository.increaseSuccess("DQLB-1");

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(queryCaptor.capture(), updateCaptor.capture(), eq(DqlRecoveryBatchEntity.class));
        assertEquals(List.of(
                DqlRecoveryBatchStatusEnum.CREATED.name(),
                DqlRecoveryBatchStatusEnum.DISPATCHED.name(),
                DqlRecoveryBatchStatusEnum.RUNNING.name()),
                queryCaptor.getValue().getQueryObject().get("status", Document.class).get("$in"));
        Document update = updateCaptor.getValue().getUpdateObject();
        assertEquals(1, update.get("$inc", Document.class).get("success_count"));
        assertTtlRefreshed(update.get("$set", Document.class));
    }

    private MongoTemplate mongoTemplate() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoTemplate.collectionExists("dql_recovery_batches")).thenReturn(true);
        when(mongoTemplate.indexOps("dql_recovery_batches")).thenReturn(indexOperations);
        return mongoTemplate;
    }

    private void assertTtlRefreshed(Document set) {
        assertInstanceOf(Date.class, set.get(DqlRecoveryBatchDto.FIELD_TTL_AT));
        assertEquals(set.get(DqlRecoveryBatchDto.FIELD_UPDATED), set.get(DqlRecoveryBatchDto.FIELD_TTL_AT));
    }
}
