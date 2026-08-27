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

        /**
         * 钉住存活阈值本身（3 分钟），而不是「恰好整 3 分钟」那个点。
         *
         * <p><b>为什么不测那个点。</b>判据是 {@code RawServerStateService:61} 的
         * {@code now - timestamp <= 3 分钟}，而 {@code now} 是<b>被测代码自己</b>再读一次
         * {@code System.currentTimeMillis()} 得到的，比本用例读到的那次晚一个未知的 {@code delta >= 0}。
         * 种子播成 {@code currentTime - 3 分钟} 时，「是否存活」等价于 {@code delta == 0} ——
         * 也就是整条调用路径是否落在同一毫秒里。这不是可判定的断言，是一次掷硬币。</p>
         *
         * <p>本用例此前正是那么写的（种子整 3 分钟、断言 not alive），靠「至少过了 1 毫秒」蒙对；
         * JVM 一热、这段路径进了同一毫秒就翻红。而且它断言的方向与生产语义（{@code <=} 即存活）
         * 以及它自己上一行的注释都相反 —— 它从来没有测过它命名的那件事。</p>
         *
         * <p><b>改成钉阈值。</b>界内 5 秒必须存活、界外 5 秒必须不存活。界外那条对任何 {@code delta}
         * 都成立；界内那条的余量是整整 5 秒（原先是 0）。阈值一旦从 3 分钟改成 2 或 4 分钟，两条各红一条。
         * 整毫秒边界上 {@code <} 与 {@code <=} 的区别用 wall clock 判定不了，也不是任何需求关心的事。</p>
         */
        @Test
        void testGetAllLatestIsAliveAroundThreeMinuteThreshold() {
            Filter filter = new Filter();
            filter.setWhere(new Where());

            long threshold = 3 * 60 * 1000L;
            long margin = 5000L;
            long currentTime = System.currentTimeMillis();

            RawServerStateDto inside = new RawServerStateDto();
            inside.setServiceId("service-inside");
            inside.setTimestamp(new Date(currentTime - threshold + margin));  // 2 分 55 秒前

            RawServerStateDto outside = new RawServerStateDto();
            outside.setServiceId("service-outside");
            outside.setTimestamp(new Date(currentTime - threshold - margin)); // 3 分 05 秒前

            List<RawServerStateDto> results = new ArrayList<>();
            results.add(inside);
            results.add(outside);

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

                // getAllLatest 按 timestamp 升序排（RawServerStateService:63），所以更早的 outside 在前。
                // 显式断言 serviceId：旧版把注释挂在了错的下标上，而两条断言恰好都是 assertFalse，
                // 于是那处错位一直没有被任何东西暴露出来。
                assertEquals("service-outside", result.getItems().get(0).getServiceId());
                assertFalse(result.getItems().get(0).getIsAlive(), "超出阈值 5 秒的服务不应判为存活");

                assertEquals("service-inside", result.getItems().get(1).getServiceId());
                assertTrue(result.getItems().get(1).getIsAlive(), "未到阈值 5 秒的服务必须判为存活");
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


