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
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlRecoveryBatchRepositoryTest {

    @Test
    @DisplayName("init ensures batch query indexes when collection already exists")
    void initEnsuresQueryIndexesWhenCollectionExists() {
        MongoTemplate mongoTemplate = mongoTemplate();

        new DqlRecoveryBatchRepository(mongoTemplate);

        verify(mongoTemplate, never()).createCollection("dql_recovery_batches");
        verify(mongoTemplate.indexOps("dql_recovery_batches")).createIndex(argThat(indexDefinition ->
                "idx_task_created".equals(indexDefinition.getIndexOptions().getString("name"))));
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

        DqlRecoveryBatchDto saved = repository.create(batch);

        assertEquals(created, saved.getCreated());
        assertEquals(created, saved.getTtlAt());
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
        assertEquals(set.get("updated"), set.get("ttl_at"));
    }

    @Test
    @DisplayName("status update only targets an active created batch")
    void statusUpdateOnlyTargetsCreatedBatch() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);

        repository.updateStatus("DQLB-1", DqlRecoveryBatchStatusEnum.DISPATCHED, null);

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).updateFirst(queryCaptor.capture(), any(Update.class), eq(DqlRecoveryBatchEntity.class));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("DQLB-1", query.get("batch_id"));
        assertEquals(List.of(DqlRecoveryBatchStatusEnum.CREATED.name()),
                query.get("status", Document.class).get("$in"));
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
        assertEquals(update.get("$set", Document.class).get("updated"),
                update.get("$set", Document.class).get("ttl_at"));
    }

    private MongoTemplate mongoTemplate() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoTemplate.collectionExists("dql_recovery_batches")).thenReturn(true);
        when(mongoTemplate.indexOps("dql_recovery_batches")).thenReturn(indexOperations);
        return mongoTemplate;
    }
}
