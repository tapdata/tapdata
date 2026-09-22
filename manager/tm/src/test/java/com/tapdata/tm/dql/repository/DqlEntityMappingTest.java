package com.tapdata.tm.dql.repository;

import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAuditEntryDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.entity.DqlEventEntity;
import com.tapdata.tm.dql.entity.DqlRecoveryBatchEntity;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexOperations;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlEntityMappingTest {
    @Test
    @DisplayName("event entity mapping preserves public and recovery fields and converts ObjectId")
    void eventEntityMappingPreservesFields() {
        MongoTemplate mongoTemplate = mongoTemplate("dql_events");
        DqlEventRepository repository = new DqlEventRepository(mongoTemplate);
        DqlEventEntity entity = new DqlEventEntity();
        entity.setId(new ObjectId("64f000000000000000000001"));
        entity.setEventId("DQL-1");
        entity.setTaskId("64f000000000000000000002");
        entity.setTaskName("sync_order");
        entity.setEventTime(new Date(1000L));
        entity.setFailedAt(new Date(2000L));
        entity.setStatus(DqlEventStatusEnum.REPROCESSING.name());
        entity.setRecoveryStatusBeforeSync(DqlEventStatusEnum.PENDING.name());
        entity.setNotReprocessableReason("DqlRecovery.Preview.TaskVersionChanged");
        entity.setPayloadData(Map.of("after", Map.of("id", 1001)));
        entity.setPayloadComplete(true);
        entity.setCurrentBatchId("DQLB-1");
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId("A-1");
        entity.setRecoveryAttempts(List.of(attempt));

        DqlEventDto dto = repository.convert(entity);

        assertEquals("64f000000000000000000001", dto.getId());
        assertEquals(entity.getEventId(), dto.getEventId());
        assertEquals(entity.getTaskId(), dto.getTaskId());
        assertEquals(entity.getEventTime(), dto.getEventTime());
        assertEquals(entity.getFailedAt(), dto.getFailedAt());
        assertEquals(entity.getStatus(), dto.getStatus());
        assertEquals(entity.getRecoveryStatusBeforeSync(), dto.getRecoveryStatusBeforeSync());
        assertEquals(entity.getNotReprocessableReason(), dto.getNotReprocessableReason());
        assertEquals(entity.getPayloadData(), dto.getPayloadData());
        assertEquals(entity.getCurrentBatchId(), dto.getCurrentBatchId());
        assertEquals("A-1", dto.getRecoveryAttempts().get(0).getAttemptId());
    }

    @Test
    @DisplayName("batch create maps DTO fields into entity and converts generated ObjectId back")
    void batchCreateMapsFieldsAndId() {
        MongoTemplate mongoTemplate = mongoTemplate("dql_recovery_batches");
        DqlRecoveryBatchRepository repository = new DqlRecoveryBatchRepository(mongoTemplate);
        when(mongoTemplate.save(any(DqlRecoveryBatchEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        DqlRecoveryBatchDto input = new DqlRecoveryBatchDto();
        input.setId("64f000000000000000000004");
        input.setBatchId("DQLB-1");
        input.setTaskId("64f000000000000000000002");
        input.setTaskName("sync_order");
        input.setEventIds(List.of("DQL-1", "DQL-2"));
        input.setOrderedEventIds(List.of("DQL-2", "DQL-1"));
        input.setSelectedCount(2);
        input.setSuccessCount(1);
        input.setFailedCount(0);
        input.setSkippedCount(1);
        input.setTaskStatusBefore("RUNNING");
        input.setTaskStatusAfter("RUNNING");
        input.setMode("AUTO");
        input.setSourceReadPauseResult("SUCCESS");
        input.setSourceReadResumeResult("SUCCESS");
        DqlRecoveryAuditEntryDto auditEntry = new DqlRecoveryAuditEntryDto();
        auditEntry.setType("BATCH_CREATED");
        input.setAuditEntries(List.of(auditEntry));

        DqlRecoveryBatchDto saved = repository.create(input);

        ArgumentCaptor<DqlRecoveryBatchEntity> captor = ArgumentCaptor.forClass(DqlRecoveryBatchEntity.class);
        verify(mongoTemplate).save(captor.capture());
        DqlRecoveryBatchEntity entity = captor.getValue();
        assertEquals(new ObjectId("64f000000000000000000004"), entity.getId());
        assertEquals(input.getBatchId(), entity.getBatchId());
        assertEquals(input.getTaskId(), entity.getTaskId());
        assertEquals(input.getEventIds(), entity.getEventIds());
        assertEquals(input.getOrderedEventIds(), entity.getOrderedEventIds());
        assertEquals(input.getSelectedCount(), entity.getSelectedCount());
        assertEquals(input.getSkippedCount(), entity.getSkippedCount());
        assertEquals(input.getTaskStatusBefore(), entity.getTaskStatusBefore());
        assertEquals(input.getTaskStatusAfter(), entity.getTaskStatusAfter());
        assertEquals(input.getMode(), entity.getMode());
        assertEquals(input.getSourceReadPauseResult(), entity.getSourceReadPauseResult());
        assertEquals(input.getSourceReadResumeResult(), entity.getSourceReadResumeResult());
        assertEquals(input.getAuditEntries(), entity.getAuditEntries());
        assertNotNull(entity.getCreated());
        assertEquals(entity.getCreated(), entity.getTtlAt());
        assertEquals("64f000000000000000000004", saved.getId());
    }

    private MongoTemplate mongoTemplate(String collectionName) {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoTemplate.collectionExists(collectionName)).thenReturn(true);
        when(mongoTemplate.indexOps(collectionName)).thenReturn(indexOperations);
        return mongoTemplate;
    }
}
