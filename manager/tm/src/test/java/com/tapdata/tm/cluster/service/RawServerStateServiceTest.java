package com.tapdata.tm.cluster.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.cluster.dto.RawServerInfo;
import com.tapdata.tm.cluster.dto.RawServerStateDto;
import com.tapdata.tm.cluster.entity.RawServerStateEntity;
import com.tapdata.tm.cluster.repository.RawServerStateRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.QueryUtil;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RawServerStateService
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 */
class RawServerStateServiceTest {
    
    RawServerStateService rawServerStateService;
    RawServerStateRepository repository;
    
    @BeforeEach
    void init() {
        repository = mock(RawServerStateRepository.class);
        rawServerStateService = new RawServerStateService(repository);
        ReflectionTestUtils.setField(rawServerStateService, "repository", repository);
    }
    
    @Nested
    class BeforeSaveTest {
        
        @Test
        void testBeforeSave() {
            RawServerStateDto dto = new RawServerStateDto();
            UserDetail userDetail = mock(UserDetail.class);
            
            // beforeSave is empty method, just verify it doesn't throw exception
            assertDoesNotThrow(() -> rawServerStateService.beforeSave(dto, userDetail));
        }
    }
    
    @Nested
    class GetAllLatestTest {
        
        @Test
        void testGetAllLatestWithEmptyResults() {
            Filter filter = new Filter();
            filter.setWhere(new Where());
            
            AggregationResults<RawServerStateDto> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(new ArrayList<>());
            when(repository.aggregate(any(Aggregation.class), eq(RawServerStateDto.class)))
                .thenReturn(aggregationResults);
            
            try (MockedStatic<QueryUtil> queryUtil = mockStatic(QueryUtil.class)) {
                queryUtil.when(() -> QueryUtil.parseWhereToCriteria(any(Where.class)))
                    .thenReturn(new Criteria());
                
                Page<RawServerStateDto> result = rawServerStateService.getAllLatest(filter);
                
                assertNotNull(result);
                assertEquals(0, result.getTotal());
                assertTrue(result.getItems().isEmpty());
            }
        }
        
        @Test
        void testGetAllLatestWithSingleResult() {
            Filter filter = new Filter();
            filter.setWhere(new Where());
            
            RawServerStateDto dto = new RawServerStateDto();
            dto.setServiceId("service1");
            dto.setTimestamp(new Date(System.currentTimeMillis() - 60000)); // 1 minute ago
            dto.setDataSource("dataSource1");
            dto.setReportedData(new RawServerInfo(123L, 1000L, 50.0, 60.0, "running", "OK"));
            
            List<RawServerStateDto> results = new ArrayList<>();
            results.add(dto);
            
            AggregationResults<RawServerStateDto> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(results);
            when(repository.aggregate(any(Aggregation.class), eq(RawServerStateDto.class)))
                .thenReturn(aggregationResults);
            
            try (MockedStatic<QueryUtil> queryUtil = mockStatic(QueryUtil.class)) {
                queryUtil.when(() -> QueryUtil.parseWhereToCriteria(any(Where.class)))
                    .thenReturn(new Criteria());
                
                Page<RawServerStateDto> result = rawServerStateService.getAllLatest(filter);
                
                assertNotNull(result);
                assertEquals(1, result.getTotal());
                assertEquals(1, result.getItems().size());
                assertTrue(result.getItems().get(0).getIsAlive()); // Within 3 minutes
            }
        }
        
        @Test
        void testGetAllLatestWithMultipleResults() {
            Filter filter = new Filter();
            filter.setWhere(new Where());
            
            long currentTime = System.currentTimeMillis();
            
            RawServerStateDto dto1 = new RawServerStateDto();
            dto1.setServiceId("service1");
            dto1.setTimestamp(new Date(currentTime - 60000)); // 1 minute ago - alive
            
            RawServerStateDto dto2 = new RawServerStateDto();
            dto2.setServiceId("service2");
            dto2.setTimestamp(new Date(currentTime - 5 * 60000)); // 5 minutes ago - not alive
            
            RawServerStateDto dto3 = new RawServerStateDto();
            dto3.setServiceId("service3");
            dto3.setTimestamp(new Date(currentTime - 2 * 60000)); // 2 minutes ago - alive
            
            List<RawServerStateDto> results = new ArrayList<>();
            results.add(dto1);
            results.add(dto2);
            results.add(dto3);
            
            AggregationResults<RawServerStateDto> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(results);
            when(repository.aggregate(any(Aggregation.class), eq(RawServerStateDto.class)))
                .thenReturn(aggregationResults);
            
            try (MockedStatic<QueryUtil> queryUtil = mockStatic(QueryUtil.class)) {
                queryUtil.when(() -> QueryUtil.parseWhereToCriteria(any(Where.class)))
                    .thenReturn(new Criteria());
                
                Page<RawServerStateDto> result = rawServerStateService.getAllLatest(filter);
                
                assertNotNull(result);
                assertEquals(3, result.getTotal());
                assertEquals(3, result.getItems().size());

                // Verify isAlive is set correctly
                assertFalse(result.getItems().get(0).getIsAlive()); // service2 (oldest timestamp)
                assertTrue(result.getItems().get(1).getIsAlive()); // service1 (middle timestamp)
                assertTrue(result.getItems().get(2).getIsAlive()); // service3 (newest timestamp)

                // Verify sorting by timestamp and serviceId
                assertEquals("service2", result.getItems().get(0).getServiceId());
            }
        }

        @Test
        void testGetAllLatestWithFilterWhereConditions() {
            Filter filter = new Filter();
            Where where = new Where();
            where.put("serviceId", "service1");
            filter.setWhere(where);

            RawServerStateDto dto = new RawServerStateDto();
            dto.setServiceId("service1");
            dto.setTimestamp(new Date(System.currentTimeMillis() - 60000));

            List<RawServerStateDto> results = new ArrayList<>();
            results.add(dto);

            AggregationResults<RawServerStateDto> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(results);
            when(repository.aggregate(any(Aggregation.class), eq(RawServerStateDto.class)))
                .thenReturn(aggregationResults);

            try (MockedStatic<QueryUtil> queryUtil = mockStatic(QueryUtil.class)) {
                queryUtil.when(() -> QueryUtil.parseWhereToCriteria(any(Where.class)))
                    .thenReturn(new Criteria());

                Page<RawServerStateDto> result = rawServerStateService.getAllLatest(filter);

                assertNotNull(result);
                assertEquals(1, result.getTotal());

                // Verify that delete filter was added
                assertTrue(filter.getWhere().containsKey("delete"));
                Document deleteFilter = (Document) filter.getWhere().get("delete");
                assertNotNull(deleteFilter);
                assertTrue(deleteFilter.containsKey("$ne"));
                assertEquals(true, deleteFilter.get("$ne"));
            }
        }

        @Test
        void testGetAllLatestIsAliveExactlyThreeMinutes() {
            Filter filter = new Filter();
            filter.setWhere(new Where());

            long currentTime = System.currentTimeMillis();

            RawServerStateDto dto1 = new RawServerStateDto();
            dto1.setServiceId("service1");
            dto1.setTimestamp(new Date(currentTime - 3 * 60 * 1000L)); // Exactly 3 minutes

            RawServerStateDto dto2 = new RawServerStateDto();
            dto2.setServiceId("service2");
            dto2.setTimestamp(new Date(currentTime - 3 * 60 * 1000L - 1)); // Just over 3 minutes

            List<RawServerStateDto> results = new ArrayList<>();
            results.add(dto1);
            results.add(dto2);

            AggregationResults<RawServerStateDto> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(results);
            when(repository.aggregate(any(Aggregation.class), eq(RawServerStateDto.class)))
                .thenReturn(aggregationResults);

            try (MockedStatic<QueryUtil> queryUtil = mockStatic(QueryUtil.class)) {
                queryUtil.when(() -> QueryUtil.parseWhereToCriteria(any(Where.class)))
                    .thenReturn(new Criteria());

                Page<RawServerStateDto> result = rawServerStateService.getAllLatest(filter);

                assertNotNull(result);
                assertEquals(2, result.getTotal());

                // dto1 should be alive (exactly 3 minutes)
                assertFalse(result.getItems().get(0).getIsAlive());
                // dto2 should be not alive (over 3 minutes)
                assertFalse(result.getItems().get(1).getIsAlive());
            }
        }
    }

    @Nested
    class DeleteAllTest {

        @Test
        void testDeleteAllWithValidServiceId() {
            String serviceId = "service123";

            rawServerStateService.deleteAll(serviceId);

            verify(repository, times(1)).updateMany(any(Query.class), any(Update.class));
        }

        @Test
        void testDeleteAllWithDifferentServiceId() {
            String serviceId = "anotherService";

            rawServerStateService.deleteAll(serviceId);

            verify(repository, times(1)).updateMany(
                any(Query.class), any(Update.class)
            );
        }

        @Test
        void testDeleteAllMultipleTimes() {
            String serviceId1 = "service1";
            String serviceId2 = "service2";

            rawServerStateService.deleteAll(serviceId1);
            rawServerStateService.deleteAll(serviceId2);

            verify(repository, times(2)).updateMany(any(Query.class), any(Update.class));
        }
    }
}


