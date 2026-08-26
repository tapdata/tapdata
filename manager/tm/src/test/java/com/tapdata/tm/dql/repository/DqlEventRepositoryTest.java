package com.tapdata.tm.dql.repository;

import com.tapdata.tm.dql.entity.DqlEventEntity;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlEventRepositoryTest {

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

    private MongoTemplate mongoTemplate() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        when(mongoTemplate.collectionExists("dql_events")).thenReturn(true);
        return mongoTemplate;
    }

    private Query capturedFindQuery(MongoTemplate mongoTemplate) {
        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).find(captor.capture(), eq(DqlEventEntity.class));
        return captor.getValue();
    }
}
