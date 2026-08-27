package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.DqlRecordIdentityTypeEnum;
import com.tapdata.tm.dql.entity.DqlEventEntity;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlEventRepositoryTest {

    @Test
    @DisplayName("init ensures record identity index when dql collection already exists")
    void initEnsuresRecordIdentityIndexWhenCollectionExists() {
        MongoTemplate mongoTemplate = mongoTemplate();

        new DqlEventRepository(mongoTemplate);

        verify(mongoTemplate, never()).createCollection("dql_events");
        verify(mongoTemplate.indexOps("dql_events")).createIndex(argThat(indexDefinition ->
                "idx_task_record_identity_event_time".equals(indexDefinition.getIndexOptions().getString("name"))));
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
                queryCaptor.getValue().getQueryObject().get("status"));
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

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(any(Query.class), updateCaptor.capture(), eq(DqlEventEntity.class));
        Document set = updateCaptor.getValue().getUpdateObject().get("$set", Document.class);
        assertEquals(DqlEventStatusEnum.RECOVERED.name(), set.get("status"));
        assertEquals(set.get("updated"), set.get("ttl_at"));
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
