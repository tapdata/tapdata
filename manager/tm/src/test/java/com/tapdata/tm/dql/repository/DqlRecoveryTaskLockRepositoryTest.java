package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.dql.entity.DqlRecoveryTaskLockEntity;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlRecoveryTaskLockRepositoryTest {

    @Test
    @DisplayName("initializes unique task and expiry indexes")
    void initializesIndexes() {
        MongoTemplate mongoTemplate = mongoTemplate();
        IndexOperations indexOperations = mongoTemplate.indexOps("dql_recovery_locks");
        ArgumentCaptor<IndexDefinition> indexCaptor = ArgumentCaptor.forClass(IndexDefinition.class);

        new DqlRecoveryTaskLockRepository(mongoTemplate);

        verify(indexOperations, org.mockito.Mockito.times(2)).createIndex(indexCaptor.capture());
        assertTrue(indexCaptor.getAllValues().stream()
                .anyMatch(index -> "uk_task_id".equals(index.getIndexOptions().getString("name"))
                        && Boolean.TRUE.equals(index.getIndexOptions().get("unique"))));
        assertTrue(indexCaptor.getAllValues().stream()
                .anyMatch(index -> "idx_expire_at".equals(index.getIndexOptions().getString("name"))));
    }

    @Test
    @DisplayName("returns false when an unexpired task lock cannot be matched")
    void rejectsUnexpiredLock() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryTaskLockRepository repository = new DqlRecoveryTaskLockRepository(mongoTemplate);
        when(mongoTemplate.findAndModify(
                any(Query.class),
                any(Update.class),
                any(FindAndModifyOptions.class),
                eq(DqlRecoveryTaskLockEntity.class)
        )).thenReturn(null);

        assertFalse(repository.tryAcquire("TASK-1", "BATCH-1",
                new Date(1000L), new Date(2000L)));
    }

    @Test
    @DisplayName("returns true when an expired task lock is atomically reclaimed")
    void reclaimsExpiredLock() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryTaskLockRepository repository = new DqlRecoveryTaskLockRepository(mongoTemplate);
        DqlRecoveryTaskLockEntity reclaimed = new DqlRecoveryTaskLockEntity();
        reclaimed.setTaskId("TASK-1");
        reclaimed.setBatchId("BATCH-2");
        when(mongoTemplate.findAndModify(
                any(Query.class),
                any(Update.class),
                any(FindAndModifyOptions.class),
                eq(DqlRecoveryTaskLockEntity.class)
        )).thenReturn(reclaimed);

        assertTrue(repository.tryAcquire("TASK-1", "BATCH-2",
                new Date(2000L), new Date(3000L)));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).findAndModify(
                queryCaptor.capture(),
                updateCaptor.capture(),
                any(FindAndModifyOptions.class),
                eq(DqlRecoveryTaskLockEntity.class)
        );
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("TASK-1", query.get("task_id"));
        assertTrue(query.toJson().contains("expire_at"));
        Document set = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals("BATCH-2", set.get("batch_id"));
        assertEquals(new Date(3000L), set.get("expire_at"));
    }

    @Test
    @DisplayName("turns a duplicate-key upsert race into a lock conflict")
    void handlesDuplicateKeyRace() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryTaskLockRepository repository = new DqlRecoveryTaskLockRepository(mongoTemplate);
        when(mongoTemplate.findAndModify(
                any(Query.class),
                any(Update.class),
                any(FindAndModifyOptions.class),
                eq(DqlRecoveryTaskLockEntity.class)
        )).thenThrow(new DuplicateKeyException("task lock already exists"));

        assertFalse(repository.tryAcquire("TASK-1", "BATCH-1",
                new Date(1000L), new Date(2000L)));
    }

    @Test
    @DisplayName("release requires the task and owning batch")
    void releasesOnlyTheOwningBatch() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryTaskLockRepository repository = new DqlRecoveryTaskLockRepository(mongoTemplate);
        when(mongoTemplate.remove(any(Query.class), eq(DqlRecoveryTaskLockEntity.class)))
                .thenReturn(DeleteResult.acknowledged(1L));

        assertTrue(repository.release("TASK-1", "BATCH-1"));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).remove(queryCaptor.capture(), eq(DqlRecoveryTaskLockEntity.class));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("TASK-1", query.get("task_id"));
        assertEquals("BATCH-1", query.get("batch_id"));
    }

    @Test
    @DisplayName("detects an unexpired task lock")
    void detectsActiveLock() {
        MongoTemplate mongoTemplate = mongoTemplate();
        DqlRecoveryTaskLockRepository repository = new DqlRecoveryTaskLockRepository(mongoTemplate);
        when(mongoTemplate.exists(any(Query.class), eq(DqlRecoveryTaskLockEntity.class))).thenReturn(true);

        assertTrue(repository.existsActive("TASK-1", new Date(2000L)));
        verify(mongoTemplate).exists(any(Query.class), eq(DqlRecoveryTaskLockEntity.class));
    }

    private MongoTemplate mongoTemplate() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoTemplate.collectionExists("dql_recovery_locks")).thenReturn(true);
        when(mongoTemplate.indexOps("dql_recovery_locks")).thenReturn(indexOperations);
        return mongoTemplate;
    }
}
