package com.tapdata.tm.apiCalls.service;

import com.tapdata.tm.apiCalls.dto.ApiCallDto;
import com.tapdata.tm.apiCalls.vo.WorkerCallsInfo;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.utils.PercentileCalculator;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.entity.ApiCallMinuteStatsEntity;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.entity.ApiCallStatsEntity;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SupplementApiCallServer
 * 
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @version v1.0 2025/9/27 Create
 */
class SupplementApiCallServerTest {
    SupplementApiCallServer supplementApiCallServer;
    
    MongoTemplate mongoOperations;
    WorkerCallServiceImpl workerCallService;
    ApiCallMinuteStatsService apiCallMinuteStatsService;
    ApiCallStatsService apiCallStatsService;
    
    @BeforeEach
    void init() {
        supplementApiCallServer = mock(SupplementApiCallServer.class);
        mongoOperations = mock(MongoTemplate.class);
        workerCallService = mock(WorkerCallServiceImpl.class);
        apiCallMinuteStatsService = mock(ApiCallMinuteStatsService.class);
        apiCallStatsService = mock(ApiCallStatsService.class);
        ReflectionTestUtils.setField(supplementApiCallServer, "mongoOperations", mongoOperations);
        ReflectionTestUtils.setField(supplementApiCallServer, "workerCallService", workerCallService);
        ReflectionTestUtils.setField(supplementApiCallServer, "apiCallMinuteStatsService", apiCallMinuteStatsService);
        ReflectionTestUtils.setField(supplementApiCallServer, "apiCallStatsService", apiCallStatsService);
    }
    
    @Nested
    class supplementOnceTest {
        
        @Test
        void testNormal() {
            when(mongoOperations.count(any(Query.class), eq(ApiCallDto.class), anyString())).thenReturn(150L);
            List<ApiCallDto> apiCalls = createTestApiCallDtos();
            when(mongoOperations.find(any(Query.class), eq(ApiCallDto.class), anyString())).thenReturn(apiCalls, Collections.emptyList());
            when(mongoOperations.updateMulti(any(Query.class), any(Update.class), anyString())).thenReturn(null);
            doCallRealMethod().when(supplementApiCallServer).supplementOnce();
            doNothing().when(supplementApiCallServer).acceptBatch(any(), any());
            doNothing().when(supplementApiCallServer).foreach(any());
            
            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("ApiCall");
                
                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.supplementOnce());
            }
        }
        
        @Test
        void testZeroCount() {
            when(mongoOperations.count(any(Query.class), eq(ApiCallDto.class), anyString())).thenReturn(0L);
            doCallRealMethod().when(supplementApiCallServer).supplementOnce();
            
            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("ApiCall");
                
                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.supplementOnce());
                verify(mongoOperations, never()).find(any(Query.class), eq(ApiCallDto.class), anyString());
            }
        }
        
        @Test
        void testEmptyApiCalls() {
            when(mongoOperations.count(any(Query.class), eq(ApiCallDto.class), anyString())).thenReturn(50L);
            when(mongoOperations.find(any(Query.class), eq(ApiCallDto.class), anyString())).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).supplementOnce();
            doNothing().when(supplementApiCallServer).acceptBatch(any(), any());
            doNothing().when(supplementApiCallServer).foreach(any());
            
            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("ApiCall");
                
                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.supplementOnce());
            }
        }
        
        @Test
        void testMultipleBatches() {
            when(mongoOperations.count(any(Query.class), eq(ApiCallDto.class), anyString())).thenReturn(250L);
            List<ApiCallDto> batch1 = createTestApiCallDtos();
            List<ApiCallDto> batch2 = createTestApiCallDtos();
            when(mongoOperations.find(any(Query.class), eq(ApiCallDto.class), anyString()))
                .thenReturn(batch1, batch2, Collections.emptyList());
            when(mongoOperations.updateMulti(any(Query.class), any(Update.class), anyString())).thenReturn(null);
            doCallRealMethod().when(supplementApiCallServer).supplementOnce();
            doNothing().when(supplementApiCallServer).acceptBatch(any(), any());
            doNothing().when(supplementApiCallServer).foreach(any());
            
            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("ApiCall");
                
                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.supplementOnce());
            }
        }
    }
    
    @Nested
    class acceptBatchTest {
        
        @Test
        void testNormal() {
            List<ApiCallDto> apiCalls = createTestApiCallDtos();
            WorkerCallsInfoGenerator generator = mock(WorkerCallsInfoGenerator.class);
            doCallRealMethod().when(supplementApiCallServer).acceptBatch(apiCalls, generator);
            
            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptBatch(apiCalls, generator));
            verify(generator, atLeastOnce()).append(any(List.class));
        }
        
        @Test
        void testWithNullElements() {
            List<ApiCallDto> apiCalls = new ArrayList<>();
            apiCalls.add(createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 60000L, 60100L));
            apiCalls.add(null);
            apiCalls.add(createApiCallDto("worker2", "api2", "gateway2", 150L, "404", 120000L, 120150L));
            
            WorkerCallsInfoGenerator generator = mock(WorkerCallsInfoGenerator.class);
            doCallRealMethod().when(supplementApiCallServer).acceptBatch(apiCalls, generator);
            
            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptBatch(apiCalls, generator));
        }
        
        @Test
        void testWithBlankWorkOid() {
            List<ApiCallDto> apiCalls = new ArrayList<>();
            ApiCallDto dto1 = createApiCallDto("", "api1", "gateway1", 100L, "200", 60000L, 60100L);
            ApiCallDto dto2 = createApiCallDto("worker1", "", "gateway1", 150L, "404", 120000L, 120150L);
            ApiCallDto dto3 = createApiCallDto("worker2", "api2", "gateway2", 200L, "500", 180000L, 180200L);
            apiCalls.add(dto1);
            apiCalls.add(dto2);
            apiCalls.add(dto3);
            
            WorkerCallsInfoGenerator generator = mock(WorkerCallsInfoGenerator.class);
            doCallRealMethod().when(supplementApiCallServer).acceptBatch(apiCalls, generator);
            
            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptBatch(apiCalls, generator));
        }
        
        @Test
        void testGroupingAndSorting() {
            List<ApiCallDto> apiCalls = new ArrayList<>();
            // 同一个worker和API，不同时间
            apiCalls.add(createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 180000L, 180100L)); // 较晚
            apiCalls.add(createApiCallDto("worker1", "api1", "gateway1", 150L, "200", 60000L, 60150L));   // 较早
            apiCalls.add(createApiCallDto("worker1", "api1", "gateway1", 120L, "200", 120000L, 120120L)); // 中间
            
            WorkerCallsInfoGenerator generator = mock(WorkerCallsInfoGenerator.class);
            doCallRealMethod().when(supplementApiCallServer).acceptBatch(apiCalls, generator);
            
            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptBatch(apiCalls, generator));
            verify(generator, atLeastOnce()).append(any(List.class));
        }
    }
    
    @Nested
    class foreachTest {
        
        @Test
        void testNormal() {
            List<ApiCallDto> apiCalls = createTestApiCallDtos();
            List<ApiCallStatsDto> callStats = createTestApiCallStatsDtos();
            List<ApiCallMinuteStatsDto> minuteStats = createTestApiCallMinuteStatsDtos();
            
            when(mongoOperations.find(any(Query.class), eq(ApiCallStatsDto.class), anyString())).thenReturn(callStats);
            when(mongoOperations.find(any(Query.class), eq(ApiCallMinuteStatsDto.class), anyString())).thenReturn(minuteStats);
            doCallRealMethod().when(supplementApiCallServer).foreach(apiCalls);
            doNothing().when(supplementApiCallServer).acceptApiCallStats(any(), any());
            doNothing().when(supplementApiCallServer).acceptApiCallMinuteStats(any(), any());
            
            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(ApiCallStatsEntity.class)).thenReturn("ApiCallStats");
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(ApiCallMinuteStatsEntity.class)).thenReturn("ApiCallMinuteStats");
                
                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.foreach(apiCalls));
            }
        }
        
        @Test
        void testEmptyList() {
            List<ApiCallDto> emptyList = Collections.emptyList();
            doCallRealMethod().when(supplementApiCallServer).foreach(emptyList);
            
            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.foreach(emptyList));
            verify(mongoOperations, never()).find(any(Query.class), any(Class.class), anyString());
        }
        
        @Test
        void testNullList() {
            doCallRealMethod().when(supplementApiCallServer).foreach(null);
            
            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.foreach(null));
            verify(mongoOperations, never()).find(any(Query.class), any(Class.class), anyString());
        }
        
        @Test
        void testWithNullElements() {
            List<ApiCallDto> apiCalls = new ArrayList<>();
            apiCalls.add(createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 60000L, 60100L));
            apiCalls.add(null);
            apiCalls.add(createApiCallDto("worker2", "api2", "gateway2", 150L, "404", 120000L, 120150L));
            
            when(mongoOperations.find(any(Query.class), eq(ApiCallStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            when(mongoOperations.find(any(Query.class), eq(ApiCallMinuteStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).foreach(apiCalls);
            doNothing().when(supplementApiCallServer).acceptApiCallStats(any(), any());
            doNothing().when(supplementApiCallServer).acceptApiCallMinuteStats(any(), any());
            
            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("TestCollection");
                
                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.foreach(apiCalls));
            }
        }
        
        @Test
        void testWithBlankApiIds() {
            List<ApiCallDto> apiCalls = new ArrayList<>();
            ApiCallDto dto1 = createApiCallDto("worker1", "", "gateway1", 100L, "200", 60000L, 60100L);
            ApiCallDto dto2 = createApiCallDto("worker2", "api2", "gateway2", 150L, "404", 120000L, 120150L);
            apiCalls.add(dto1);
            apiCalls.add(dto2);
            
            when(mongoOperations.find(any(Query.class), eq(ApiCallStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            when(mongoOperations.find(any(Query.class), eq(ApiCallMinuteStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).foreach(apiCalls);
            doNothing().when(supplementApiCallServer).acceptApiCallStats(any(), any());
            doNothing().when(supplementApiCallServer).acceptApiCallMinuteStats(any(), any());
            
            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("TestCollection");
                
                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.foreach(apiCalls));
            }
        }
        
        @Test
        void testWithNullReqTime() {
            List<ApiCallDto> apiCalls = new ArrayList<>();
            ApiCallDto dto1 = createApiCallDto("worker1", "api1", "gateway1", 100L, "200", null, 60100L);
            ApiCallDto dto2 = createApiCallDto("worker2", "api2", "gateway2", 150L, "404", 120000L, 120150L);
            apiCalls.add(dto1);
            apiCalls.add(dto2);
            
            when(mongoOperations.find(any(Query.class), eq(ApiCallStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            when(mongoOperations.find(any(Query.class), eq(ApiCallMinuteStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).foreach(apiCalls);
            doNothing().when(supplementApiCallServer).acceptApiCallStats(any(), any());
            doNothing().when(supplementApiCallServer).acceptApiCallMinuteStats(any(), any());
            
            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("TestCollection");
                
                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.foreach(apiCalls));
            }
        }
        
        @Test
        void testBulkWriteCalled() {
            List<ApiCallDto> apiCalls = createTestApiCallDtos();
            Map<String, ApiCallStatsDto> callMap = new HashMap<>();
            callMap.put("api1", new ApiCallStatsDto());
            Map<String, ApiCallMinuteStatsDto> callMinuteMap = new HashMap<>();
            callMinuteMap.put("api1_60000", new ApiCallMinuteStatsDto());
            
            when(mongoOperations.find(any(Query.class), eq(ApiCallStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            when(mongoOperations.find(any(Query.class), eq(ApiCallMinuteStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).foreach(apiCalls);
            doAnswer(invocation -> {
                Map<String, ApiCallStatsDto> map = invocation.getArgument(1);
                map.put("api1", new ApiCallStatsDto());
                return null;
            }).when(supplementApiCallServer).acceptApiCallStats(any(), any());
            doAnswer(invocation -> {
                Map<String, ApiCallMinuteStatsDto> map = invocation.getArgument(1);
                map.put("api1_60000", new ApiCallMinuteStatsDto());
                return null;
            }).when(supplementApiCallServer).acceptApiCallMinuteStats(any(), any());
            
            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("TestCollection");
                
                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.foreach(apiCalls));
                verify(apiCallStatsService, atLeastOnce()).bulkWrite(any(List.class), eq(ApiCallStatsEntity.class), any(Function.class));
                verify(apiCallMinuteStatsService, atLeastOnce()).bulkWrite(any(List.class), eq(ApiCallMinuteStatsEntity.class), any(Function.class));
            }
        }
    }
    
    // Helper methods
    private List<ApiCallDto> createTestApiCallDtos() {
        List<ApiCallDto> dtos = new ArrayList<>();
        dtos.add(createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 60000L, 60100L));
        dtos.add(createApiCallDto("worker1", "api2", "gateway1", 200L, "404", 60000L, 60200L));
        dtos.add(createApiCallDto("worker2", "api1", "gateway2", 150L, "500", 120000L, 120150L));
        return dtos;
    }
    
    private ApiCallDto createApiCallDto(String workOid, String allPathId, String apiGatewayUuid, 
                                       Long latency, String code, Long reqTime, Long resTime) {
        ApiCallDto dto = new ApiCallDto();
        dto.setId(new ObjectId());
        dto.setWorkOid(workOid);
        dto.setAllPathId(allPathId);
        dto.setApi_gateway_uuid(apiGatewayUuid);
        dto.setLatency(latency);
        dto.setCode(code);
        dto.setCodeMsg("OK".equals(code) ? "OK" : "ERROR");
        dto.setReqTime(reqTime);
        dto.setResTime(resTime);
        dto.setReqBytes(1024L);
        dto.setResRows(10L);
        Map<String, Object> userInfo = new HashMap<>();
        userInfo.put("clientId", "client123");
        dto.setUserInfo(userInfo);
        return dto;
    }
    
    private List<ApiCallStatsDto> createTestApiCallStatsDtos() {
        List<ApiCallStatsDto> dtos = new ArrayList<>();
        ApiCallStatsDto dto = new ApiCallStatsDto();
        dto.setModuleId("api1");
        dto.setCallTotalCount(10L);
        dtos.add(dto);
        return dtos;
    }
    
    private List<ApiCallMinuteStatsDto> createTestApiCallMinuteStatsDtos() {
        List<ApiCallMinuteStatsDto> dtos = new ArrayList<>();
        ApiCallMinuteStatsDto dto = new ApiCallMinuteStatsDto();
        dto.setModuleId("api1");
        dto.setApiCallTime(new Date(60000L));
        dtos.add(dto);
        return dtos;
    }

    @Nested
    class acceptApiCallStatsTest {

        @Test
        void testNormal() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 60000L, 60100L);
            apiCall.setCodeMsg("OK");
            Map<String, ApiCallStatsDto> callMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallStats(apiCall, callMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallStats(apiCall, callMap));
            Assertions.assertEquals(1, callMap.size());
            Assertions.assertTrue(callMap.containsKey("api1"));

            ApiCallStatsDto stats = callMap.get("api1");
            Assertions.assertEquals("api1", stats.getModuleId());
            Assertions.assertEquals(1L, stats.getCallTotalCount());
            Assertions.assertEquals(1024L, stats.getTransferDataTotalBytes());
            Assertions.assertEquals(0L, stats.getCallAlarmTotalCount());
            Assertions.assertEquals(10L, stats.getResponseDataRowTotalCount());
            Assertions.assertEquals(100L, stats.getTotalResponseTime());
            Assertions.assertEquals(100L, stats.getMaxResponseTime());
            Assertions.assertEquals(0.0, stats.getAccessFailureRate());
            Assertions.assertEquals(1, stats.getClientIds().size());
            Assertions.assertTrue(stats.getClientIds().contains("client123"));
        }

        @Test
        void testErrorCall() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", 150L, "404", 60000L, 60150L);
            apiCall.setCodeMsg("NOT_FOUND");
            Map<String, ApiCallStatsDto> callMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallStats(apiCall, callMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallStats(apiCall, callMap));

            ApiCallStatsDto stats = callMap.get("api1");
            Assertions.assertEquals(1L, stats.getCallAlarmTotalCount());
            Assertions.assertEquals(1.0, stats.getAccessFailureRate());
            Assertions.assertEquals(1L, stats.getAlarmApiTotalCount());
        }

        @Test
        void testExistingStats() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", 200L, "200", 60000L, 60200L);
            apiCall.setCodeMsg("OK");
            Map<String, ApiCallStatsDto> callMap = new HashMap<>();

            // 预先存在的统计数据
            ApiCallStatsDto existingStats = new ApiCallStatsDto();
            existingStats.setModuleId("api1");
            existingStats.setCallTotalCount(5L);
            existingStats.setTransferDataTotalBytes(2048L);
            existingStats.setCallAlarmTotalCount(1L);
            existingStats.setResponseDataRowTotalCount(50L);
            existingStats.setTotalResponseTime(500L);
            existingStats.setMaxResponseTime(150L);
            Set<String> clientIds = new HashSet<>();
            clientIds.add("existingClient");
            existingStats.setClientIds(clientIds);
            callMap.put("api1", existingStats);

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallStats(apiCall, callMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallStats(apiCall, callMap));

            ApiCallStatsDto stats = callMap.get("api1");
            Assertions.assertEquals(6L, stats.getCallTotalCount());
            Assertions.assertEquals(3072L, stats.getTransferDataTotalBytes());
            Assertions.assertEquals(1L, stats.getCallAlarmTotalCount());
            Assertions.assertEquals(60L, stats.getResponseDataRowTotalCount());
            Assertions.assertEquals(700L, stats.getTotalResponseTime());
            Assertions.assertEquals(200L, stats.getMaxResponseTime());
            Assertions.assertEquals(2, stats.getClientIds().size());
            Assertions.assertTrue(stats.getClientIds().contains("client123"));
            Assertions.assertTrue(stats.getClientIds().contains("existingClient"));
        }

        @Test
        void testNullValues() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", null, "200", 60000L, 60100L);
            apiCall.setCodeMsg("OK");
            apiCall.setReqBytes(null);
            apiCall.setResRows(null);
            apiCall.setUserInfo(null);
            Map<String, ApiCallStatsDto> callMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallStats(apiCall, callMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallStats(apiCall, callMap));

            ApiCallStatsDto stats = callMap.get("api1");
            Assertions.assertEquals(1L, stats.getCallTotalCount());
            Assertions.assertEquals(0L, stats.getTransferDataTotalBytes());
            Assertions.assertEquals(0L, stats.getResponseDataRowTotalCount());
            Assertions.assertEquals(0L, stats.getTotalResponseTime());
            Assertions.assertEquals(0L, stats.getMaxResponseTime());
            Assertions.assertTrue(stats.getClientIds().isEmpty());
        }

        @Test
        void testUserInfoWithoutClientId() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 60000L, 60100L);
            apiCall.setCodeMsg("OK");
            Map<String, Object> userInfo = new HashMap<>();
            userInfo.put("userId", "user123");
            apiCall.setUserInfo(userInfo);
            Map<String, ApiCallStatsDto> callMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallStats(apiCall, callMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallStats(apiCall, callMap));

            ApiCallStatsDto stats = callMap.get("api1");
            Assertions.assertTrue(stats.getClientIds().isEmpty());
        }

        @Test
        void testUserInfoWithNullClientId() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 60000L, 60100L);
            apiCall.setCodeMsg("OK");
            Map<String, Object> userInfo = new HashMap<>();
            userInfo.put("clientId", null);
            apiCall.setUserInfo(userInfo);
            Map<String, ApiCallStatsDto> callMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallStats(apiCall, callMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallStats(apiCall, callMap));

            ApiCallStatsDto stats = callMap.get("api1");
            Assertions.assertEquals(0, stats.getClientIds().size());
            Assertions.assertFalse(stats.getClientIds().contains("null"));
        }
    }

    @Nested
    class acceptApiCallMinuteStatsTest {

        @Test
        void testNormal() {
            ApiCallDto apiCall = createApiCallDto("worker1", "68d8f2735f56dc41d2747d9d", "gateway1", 100L, "200", 60000L, 60100L);
            Map<String, ApiCallMinuteStatsDto> callMinuteMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallMinuteStats(apiCall, callMinuteMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallMinuteStats(apiCall, callMinuteMap));
            Assertions.assertEquals(1, callMinuteMap.size());
            Assertions.assertTrue(callMinuteMap.containsKey("68d8f2735f56dc41d2747d9d_60000"));

            ApiCallMinuteStatsDto stats = callMinuteMap.get("68d8f2735f56dc41d2747d9d_60000");
            Assertions.assertEquals("68d8f2735f56dc41d2747d9d", stats.getModuleId());
            Assertions.assertEquals(new Date(60000L), stats.getApiCallTime());
            Assertions.assertEquals(10L, stats.getResponseDataRowTotalCount());
            Assertions.assertEquals(100L, stats.getTotalResponseTime());
            Assertions.assertEquals(1024L, stats.getTransferDataTotalBytes());
            Assertions.assertEquals(10.0, stats.getResponseTimePerRow());
            Assertions.assertEquals(10240.0, stats.getTransferBytePerSecond());
            Assertions.assertEquals(apiCall.getId().toHexString(), stats.getLastApiCallId());
        }

        @Test
        void testNullReqTime() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", 100L, "200", null, 60100L);
            Map<String, ApiCallMinuteStatsDto> callMinuteMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallMinuteStats(apiCall, callMinuteMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallMinuteStats(apiCall, callMinuteMap));
            Assertions.assertTrue(callMinuteMap.isEmpty());
        }

        @Test
        void testExistingStats() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", 150L, "200", 60000L, 60150L);
            Map<String, ApiCallMinuteStatsDto> callMinuteMap = new HashMap<>();

            // 预先存在的统计数据
            ApiCallMinuteStatsDto existingStats = new ApiCallMinuteStatsDto();
            existingStats.setModuleId("api1");
            existingStats.setApiCallTime(new Date(60000L));
            existingStats.setResponseDataRowTotalCount(20L);
            existingStats.setTotalResponseTime(200L);
            existingStats.setTransferDataTotalBytes(2048L);
            callMinuteMap.put("api1_60000", existingStats);

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallMinuteStats(apiCall, callMinuteMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallMinuteStats(apiCall, callMinuteMap));

            ApiCallMinuteStatsDto stats = callMinuteMap.get("api1_60000");
            Assertions.assertEquals(30L, stats.getResponseDataRowTotalCount());
            Assertions.assertEquals(350L, stats.getTotalResponseTime());
            Assertions.assertEquals(3072L, stats.getTransferDataTotalBytes());
            Assertions.assertEquals(11.666666666666666, stats.getResponseTimePerRow(), 0.001);
            Assertions.assertEquals(8777.142857142857, stats.getTransferBytePerSecond(), 0.001);
        }

        @Test
        void testNullValues() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", null, "200", 60000L, 60100L);
            apiCall.setReqBytes(null);
            apiCall.setResRows(null);
            Map<String, ApiCallMinuteStatsDto> callMinuteMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallMinuteStats(apiCall, callMinuteMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallMinuteStats(apiCall, callMinuteMap));

            ApiCallMinuteStatsDto stats = callMinuteMap.get("api1_60000");
            Assertions.assertEquals(0L, stats.getResponseDataRowTotalCount());
            Assertions.assertEquals(0L, stats.getTotalResponseTime());
            Assertions.assertEquals(0L, stats.getTransferDataTotalBytes());
            Assertions.assertEquals(0.0, stats.getResponseTimePerRow());
            Assertions.assertEquals(0.0, stats.getTransferBytePerSecond());
        }

        @Test
        void testZeroDivision() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", 0L, "200", 60000L, 60100L);
            apiCall.setResRows(0L);
            Map<String, ApiCallMinuteStatsDto> callMinuteMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallMinuteStats(apiCall, callMinuteMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallMinuteStats(apiCall, callMinuteMap));

            ApiCallMinuteStatsDto stats = callMinuteMap.get("api1_60000");
            Assertions.assertEquals(0.0, stats.getResponseTimePerRow());
            Assertions.assertEquals(0.0, stats.getTransferBytePerSecond());
        }

        @Test
        void testMinuteCalculation() {
            // 测试不同时间的分钟计算
            ApiCallDto apiCall1 = createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 65432L, 65532L);
            ApiCallDto apiCall2 = createApiCallDto("worker1", "api1", "gateway1", 150L, "200", 125678L, 125828L);
            Map<String, ApiCallMinuteStatsDto> callMinuteMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallMinuteStats(any(), any());

            Assertions.assertDoesNotThrow(() -> {
                supplementApiCallServer.acceptApiCallMinuteStats(apiCall1, callMinuteMap);
                supplementApiCallServer.acceptApiCallMinuteStats(apiCall2, callMinuteMap);
            });

            // 65432 / 60000 * 60000 = 60000
            // 125678 / 60000 * 60000 = 120000
            Assertions.assertTrue(callMinuteMap.containsKey("api1_60000"));
            Assertions.assertTrue(callMinuteMap.containsKey("api1_120000"));
            Assertions.assertEquals(new Date(60000L), callMinuteMap.get("api1_60000").getApiCallTime());
            Assertions.assertEquals(new Date(120000L), callMinuteMap.get("api1_120000").getApiCallTime());
        }
    }

    @Nested
    class convertToWorkerCallsInfoTest {

        @Test
        void testNormal() {
            ApiCallDto dto = createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 60000L, 60100L);

            WorkerCallsInfo result = ReflectionTestUtils.invokeMethod(supplementApiCallServer, "convertToWorkerCallsInfo", dto);

            Assertions.assertNotNull(result);
            Assertions.assertEquals("worker1", result.getWorkOid());
            Assertions.assertEquals("gateway1", result.getApiGatewayUuid());
            Assertions.assertEquals("api1", result.getApiId());
            Assertions.assertEquals(100L, result.getLatency());
            Assertions.assertEquals("200", result.getCode());
            Assertions.assertEquals(60000L, result.getReqTime());
            Assertions.assertEquals(60100L, result.getResTime());
        }

        @Test
        void testNullValues() {
            ApiCallDto dto = new ApiCallDto();
            dto.setWorkOid(null);
            dto.setApi_gateway_uuid(null);
            dto.setAllPathId(null);
            dto.setLatency(null);
            dto.setCode(null);
            dto.setReqTime(null);
            dto.setResTime(null);

            WorkerCallsInfo result = ReflectionTestUtils.invokeMethod(supplementApiCallServer, "convertToWorkerCallsInfo", dto);

            Assertions.assertNotNull(result);
            Assertions.assertNull(result.getWorkOid());
            Assertions.assertNull(result.getApiGatewayUuid());
            Assertions.assertNull(result.getApiId());
            Assertions.assertNull(result.getLatency());
            Assertions.assertNull(result.getCode());
            Assertions.assertNull(result.getReqTime());
            Assertions.assertNull(result.getResTime());
        }
    }

    @Nested
    class sortAndProcessListTest {

        @Test
        void testNormal() {
            List<WorkerCallsInfo> list = new ArrayList<>();
            WorkerCallsInfo info1 = new WorkerCallsInfo();
            info1.setReqTime(180000L);
            WorkerCallsInfo info2 = new WorkerCallsInfo();
            info2.setReqTime(60000L);
            WorkerCallsInfo info3 = new WorkerCallsInfo();
            info3.setReqTime(120000L);
            list.add(info1);
            list.add(info2);
            list.add(info3);

            List<WorkerCallsInfo> result = ReflectionTestUtils.invokeMethod(supplementApiCallServer, "sortAndProcessList", list);

            Assertions.assertNotNull(result);
            Assertions.assertEquals(3, result.size());
            Assertions.assertEquals(60000L, result.get(0).getReqTime());
            Assertions.assertEquals(120000L, result.get(1).getReqTime());
            Assertions.assertEquals(180000L, result.get(2).getReqTime());
        }

        @Test
        void testEmptyList() {
            List<WorkerCallsInfo> emptyList = new ArrayList<>();

            List<WorkerCallsInfo> result = ReflectionTestUtils.invokeMethod(supplementApiCallServer, "sortAndProcessList", emptyList);

            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.isEmpty());
        }

        @Test
        void testSingleElement() {
            List<WorkerCallsInfo> list = new ArrayList<>();
            WorkerCallsInfo info = new WorkerCallsInfo();
            info.setReqTime(60000L);
            list.add(info);

            List<WorkerCallsInfo> result = ReflectionTestUtils.invokeMethod(supplementApiCallServer, "sortAndProcessList", list);

            Assertions.assertNotNull(result);
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(60000L, result.get(0).getReqTime());
        }

        @Test
        void testNullReqTime() {
            List<WorkerCallsInfo> list = new ArrayList<>();
            WorkerCallsInfo info1 = new WorkerCallsInfo();
            info1.setReqTime(null);
            WorkerCallsInfo info2 = new WorkerCallsInfo();
            info2.setReqTime(60000L);
            list.add(info1);
            list.add(info2);

            // 这个测试可能会抛出NullPointerException，因为Comparator.comparing不处理null
            Assertions.assertThrows(NullPointerException.class, () -> {
                ReflectionTestUtils.invokeMethod(supplementApiCallServer, "sortAndProcessList", list);
            });
        }
    }

    @Nested
    class callUpdateTest {

        @Test
        void testNormal() {
            List<WorkerCallEntity> infos = createTestWorkerCallEntities();
            List<WorkerCallEntity> existingEntities = createExistingWorkerCallEntities();

            when(mongoOperations.find(any(Query.class), eq(WorkerCallEntity.class))).thenReturn(existingEntities);
            doCallRealMethod().when(supplementApiCallServer).callUpdate(infos);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), eq(0.5d))).thenReturn(100L);
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), eq(0.95d))).thenReturn(180L);
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), eq(0.99d))).thenReturn(200L);

                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(infos));
                verify(workerCallService, times(1)).bulkUpsert(infos);
            }
        }

        @Test
        void testEmptyList() {
            List<WorkerCallEntity> emptyList = Collections.emptyList();
            doCallRealMethod().when(supplementApiCallServer).callUpdate(emptyList);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(emptyList));
            verify(mongoOperations, never()).find(any(Query.class), eq(WorkerCallEntity.class));
            verify(workerCallService, never()).bulkUpsert(any());
        }

        @Test
        void testNullList() {
            doCallRealMethod().when(supplementApiCallServer).callUpdate(null);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(null));
            verify(mongoOperations, never()).find(any(Query.class), eq(WorkerCallEntity.class));
            verify(workerCallService, never()).bulkUpsert(any());
        }

        @Test
        void testNoExistingEntities() {
            List<WorkerCallEntity> infos = createTestWorkerCallEntities();

            when(mongoOperations.find(any(Query.class), eq(WorkerCallEntity.class))).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).callUpdate(infos);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(null);

                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(infos));
                verify(workerCallService, times(1)).bulkUpsert(infos);
            }
        }

        @Test
        void testNullDelays() {
            List<WorkerCallEntity> infos = createTestWorkerCallEntities();
            infos.get(0).setDelays(null);
            List<WorkerCallEntity> existingEntities = createExistingWorkerCallEntities();

            when(mongoOperations.find(any(Query.class), eq(WorkerCallEntity.class))).thenReturn(existingEntities);
            doCallRealMethod().when(supplementApiCallServer).callUpdate(infos);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(null);

                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(infos));
            }
        }

        @Test
        void testZeroReqCount() {
            List<WorkerCallEntity> infos = createTestWorkerCallEntities();
            infos.get(0).setReqCount(0L);
            infos.get(0).setErrorCount(0L);

            when(mongoOperations.find(any(Query.class), eq(WorkerCallEntity.class))).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).callUpdate(infos);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(null);

                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(infos));

                WorkerCallEntity entity = infos.get(0);
                Assertions.assertEquals(0.0, entity.getErrorRate());
                Assertions.assertEquals(0.0, entity.getRps());
            }
        }

        @Test
        void testErrorRateCalculation() {
            List<WorkerCallEntity> infos = createTestWorkerCallEntities();
            WorkerCallEntity entity = infos.get(0);
            entity.setReqCount(10L);
            entity.setErrorCount(3L);

            when(mongoOperations.find(any(Query.class), eq(WorkerCallEntity.class))).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).callUpdate(infos);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(infos));

                // errorRate = 0.1 * errorCount / reqCount = 0.1 * 3 / 10 = 0.03
                Assertions.assertEquals(0.03, entity.getErrorRate(), 0.001);
                // rps = reqCount / 60 = 10 / 60 = 0.16666...
                Assertions.assertEquals(0.16666666666666666, entity.getRps(), 0.001);
            }
        }

        @Test
        void testMergeWithExistingData() {
            List<WorkerCallEntity> infos = createTestWorkerCallEntities();
            WorkerCallEntity newEntity = infos.get(0);
            newEntity.setWorkOid("worker1");
            newEntity.setApiId("api1");
            newEntity.setErrorCount(2L);
            newEntity.setReqCount(5L);
            List<Long> newDelays = new ArrayList<>(Arrays.asList(100L, 150L));
            newEntity.setDelays(newDelays);

            List<WorkerCallEntity> existingEntities = new ArrayList<>();
            WorkerCallEntity existingEntity = new WorkerCallEntity();
            existingEntity.setWorkOid("worker1");
            existingEntity.setApiId("api1");
            existingEntity.setErrorCount(3L);
            existingEntity.setReqCount(8L);
            List<Long> existingDelays = new ArrayList<>(Arrays.asList(80L, 120L, 200L));
            existingEntity.setDelays(existingDelays);
            existingEntities.add(existingEntity);

            when(mongoOperations.find(any(Query.class), eq(WorkerCallEntity.class))).thenReturn(existingEntities);
            doCallRealMethod().when(supplementApiCallServer).callUpdate(infos);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(120L);

                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(infos));

                // 验证数据合并
                Assertions.assertEquals(13L, newEntity.getReqCount()); // 5 + 8
                Assertions.assertEquals(5, newEntity.getDelays().size()); // 2 + 3
                Assertions.assertTrue(newEntity.getDelays().containsAll(new ArrayList<>(Arrays.asList(100L, 150L, 80L, 120L, 200L))));
            }
        }
    }

    // Helper methods for callUpdate tests
    private List<WorkerCallEntity> createTestWorkerCallEntities() {
        List<WorkerCallEntity> entities = new ArrayList<>();
        WorkerCallEntity entity = new WorkerCallEntity();
        entity.setWorkOid("worker1");
        entity.setApiId("api1");
        entity.setProcessId("process1");
        entity.setTimeStart(60000L);
        entity.setTimeGranularity(60);
        entity.setDelete(false);
        entity.setErrorCount(1L);
        entity.setReqCount(10L);
        entity.setDelays(new ArrayList<>(Arrays.asList(100L, 150L, 200L)));
        entities.add(entity);
        return entities;
    }

    private List<WorkerCallEntity> createExistingWorkerCallEntities() {
        List<WorkerCallEntity> entities = new ArrayList<>();
        WorkerCallEntity entity = new WorkerCallEntity();
        entity.setWorkOid("worker1");
        entity.setApiId("api1");
        entity.setErrorCount(2L);
        entity.setReqCount(5L);
        entity.setDelays(new ArrayList<>(Arrays.asList(80L, 120L)));
        entities.add(entity);
        return entities;
    }

    @Nested
    class edgeCasesTest {

        @Test
        void testSupplementOnceWithLargeDataset() {
            when(mongoOperations.count(any(Query.class), eq(ApiCallDto.class), anyString())).thenReturn(1000L);
            List<ApiCallDto> batch1 = createLargeApiCallDtos(100);
            List<ApiCallDto> batch2 = createLargeApiCallDtos(100);
            when(mongoOperations.find(any(Query.class), eq(ApiCallDto.class), anyString()))
                .thenReturn(batch1, batch2, Collections.emptyList());
            when(mongoOperations.updateMulti(any(Query.class), any(Update.class), anyString())).thenReturn(null);
            doCallRealMethod().when(supplementApiCallServer).supplementOnce();
            doNothing().when(supplementApiCallServer).acceptBatch(any(), any());
            doNothing().when(supplementApiCallServer).foreach(any());

            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("ApiCall");

                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.supplementOnce());
                verify(supplementApiCallServer, times(1)).acceptBatch(any(), any());
                verify(supplementApiCallServer, times(1)).foreach(any());
            }
        }

        @Test
        void testAcceptBatchWithMixedData() {
            List<ApiCallDto> apiCalls = new ArrayList<>();
            // 正常数据
            apiCalls.add(createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 60000L, 60100L));
            // null元素
            apiCalls.add(null);
            // 空字符串
            apiCalls.add(createApiCallDto("", "api2", "gateway1", 150L, "404", 60000L, 60150L));
            apiCalls.add(createApiCallDto("worker1", "", "gateway1", 200L, "500", 60000L, 60200L));
            // 正常数据
            apiCalls.add(createApiCallDto("worker2", "api3", "gateway2", 120L, "200", 120000L, 120120L));

            WorkerCallsInfoGenerator generator = mock(WorkerCallsInfoGenerator.class);
            doCallRealMethod().when(supplementApiCallServer).acceptBatch(apiCalls, generator);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptBatch(apiCalls, generator));
        }

        @Test
        void testForeachWithComplexScenario() {
            List<ApiCallDto> apiCalls = new ArrayList<>();
            // 不同API，不同时间
            apiCalls.add(createApiCallDto("worker1", "api1", "gateway1", 100L, "200", 60000L, 60100L));
            apiCalls.add(createApiCallDto("worker1", "api1", "gateway1", 150L, "404", 65000L, 65150L)); // 同一分钟
            apiCalls.add(createApiCallDto("worker2", "api2", "gateway2", 200L, "500", 120000L, 120200L)); // 不同分钟
            apiCalls.add(createApiCallDto("worker2", "api2", "gateway2", 120L, "200", 125000L, 125120L)); // 同一分钟

            when(mongoOperations.find(any(Query.class), eq(ApiCallStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            when(mongoOperations.find(any(Query.class), eq(ApiCallMinuteStatsDto.class), anyString())).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).foreach(apiCalls);
            doAnswer(invocation -> {
                Map<String, ApiCallStatsDto> map = invocation.getArgument(1);
                ApiCallDto call = invocation.getArgument(0);
                if (call.getAllPathId() != null) {
                    map.put(call.getAllPathId(), new ApiCallStatsDto());
                }
                return null;
            }).when(supplementApiCallServer).acceptApiCallStats(any(), any());
            doAnswer(invocation -> {
                Map<String, ApiCallMinuteStatsDto> map = invocation.getArgument(1);
                ApiCallDto call = invocation.getArgument(0);
                if (call.getAllPathId() != null && call.getReqTime() != null) {
                    long minute = call.getReqTime() / 60000L * 60000L;
                    map.put(call.getAllPathId() + "_" + minute, new ApiCallMinuteStatsDto());
                }
                return null;
            }).when(supplementApiCallServer).acceptApiCallMinuteStats(any(), any());

            try (MockedStatic<MongoUtils> mockedMongoUtils = mockStatic(MongoUtils.class)) {
                mockedMongoUtils.when(() -> MongoUtils.getCollectionName(any())).thenReturn("TestCollection");

                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.foreach(apiCalls));
                verify(apiCallStatsService, times(1)).bulkWrite(any(List.class), eq(ApiCallStatsEntity.class), any(Function.class));
                verify(apiCallMinuteStatsService, times(1)).bulkWrite(any(List.class), eq(ApiCallMinuteStatsEntity.class), any(Function.class));
            }
        }

        @Test
        void testCallUpdateWithComplexMerging() {
            List<WorkerCallEntity> infos = new ArrayList<>();

            // 第一个实体
            WorkerCallEntity entity1 = new WorkerCallEntity();
            entity1.setWorkOid("worker1");
            entity1.setApiId("api1");
            entity1.setProcessId("process1");
            entity1.setTimeStart(60000L);
            entity1.setTimeGranularity(60);
            entity1.setDelete(false);
            entity1.setErrorCount(2L);
            entity1.setReqCount(10L);
            entity1.setDelays(new ArrayList<>(Arrays.asList(100L, 150L)));
            infos.add(entity1);

            // 第二个实体
            WorkerCallEntity entity2 = new WorkerCallEntity();
            entity2.setWorkOid("worker2");
            entity2.setApiId("api2");
            entity2.setProcessId("process2");
            entity2.setTimeStart(120000L);
            entity2.setTimeGranularity(60);
            entity2.setDelete(false);
            entity2.setErrorCount(1L);
            entity2.setReqCount(5L);
            entity2.setDelays(new ArrayList<>(Arrays.asList(80L, 200L)));
            infos.add(entity2);

            // 模拟已存在的数据
            List<WorkerCallEntity> existingEntities = new ArrayList<>();
            WorkerCallEntity existing1 = new WorkerCallEntity();
            existing1.setWorkOid("worker1");
            existing1.setApiId("api1");
            existing1.setErrorCount(3L);
            existing1.setReqCount(8L);
            existing1.setDelays(new ArrayList<>(Arrays.asList(90L, 110L, 180L)));
            existingEntities.add(existing1);

            when(mongoOperations.find(any(Query.class), eq(WorkerCallEntity.class))).thenReturn(existingEntities);
            doCallRealMethod().when(supplementApiCallServer).callUpdate(infos);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), eq(0.5d))).thenReturn(120L);
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), eq(0.95d))).thenReturn(180L);
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), eq(0.99d))).thenReturn(200L);

                Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(infos));

                // 验证第一个实体的合并结果
                Assertions.assertEquals(13L, entity1.getReqCount()); // 10 + 8
                Assertions.assertEquals(5, entity1.getDelays().size()); // 2 + 3
                Assertions.assertEquals(120L, entity1.getP50());
                Assertions.assertEquals(180L, entity1.getP95());
                Assertions.assertEquals(200L, entity1.getP99());

                // 验证第二个实体（没有已存在数据）
                Assertions.assertEquals(5L, entity2.getReqCount());
                Assertions.assertEquals(2, entity2.getDelays().size());
                Assertions.assertEquals(120L, entity2.getP50());
            }
        }

        @Test
        void testAcceptApiCallStatsWithExtremeValues() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", Long.MAX_VALUE, "200", 60000L, 60100L);
            apiCall.setCodeMsg("OK");
            apiCall.setReqBytes(Long.MAX_VALUE);
            apiCall.setResRows(Long.MAX_VALUE);
            Map<String, ApiCallStatsDto> callMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallStats(apiCall, callMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallStats(apiCall, callMap));

            ApiCallStatsDto stats = callMap.get("api1");
            Assertions.assertEquals(1L, stats.getCallTotalCount());
            Assertions.assertEquals(Long.MAX_VALUE, stats.getTransferDataTotalBytes());
            Assertions.assertEquals(Long.MAX_VALUE, stats.getResponseDataRowTotalCount());
            Assertions.assertEquals(Long.MAX_VALUE, stats.getTotalResponseTime());
            Assertions.assertEquals(Long.MAX_VALUE, stats.getMaxResponseTime());
        }

        @Test
        void testAcceptApiCallMinuteStatsWithExtremeValues() {
            ApiCallDto apiCall = createApiCallDto("worker1", "api1", "gateway1", Long.MAX_VALUE, "200", 60000L, 60100L);
            apiCall.setReqBytes(Long.MAX_VALUE);
            apiCall.setResRows(Long.MAX_VALUE);
            Map<String, ApiCallMinuteStatsDto> callMinuteMap = new HashMap<>();

            doCallRealMethod().when(supplementApiCallServer).acceptApiCallMinuteStats(apiCall, callMinuteMap);

            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.acceptApiCallMinuteStats(apiCall, callMinuteMap));

            ApiCallMinuteStatsDto stats = callMinuteMap.get("api1_60000");
            Assertions.assertEquals(Long.MAX_VALUE, stats.getResponseDataRowTotalCount());
            Assertions.assertEquals(Long.MAX_VALUE, stats.getTotalResponseTime());
            Assertions.assertEquals(Long.MAX_VALUE, stats.getTransferDataTotalBytes());
            // 极值情况下的计算
            Assertions.assertEquals(1.0, stats.getResponseTimePerRow());
            Assertions.assertEquals(1000.0, stats.getTransferBytePerSecond());
        }

        @Test
        void testCallUpdateWithNullAndEmptyDelays() {
            List<WorkerCallEntity> infos = new ArrayList<>();

            WorkerCallEntity entity1 = new WorkerCallEntity();
            entity1.setWorkOid("worker1");
            entity1.setApiId("api1");
            entity1.setDelays(null); // null delays
            infos.add(entity1);

            WorkerCallEntity entity2 = new WorkerCallEntity();
            entity2.setWorkOid("worker2");
            entity2.setApiId("api2");
            entity2.setDelays(new ArrayList<>()); // empty delays
            infos.add(entity2);

            when(mongoOperations.find(any(Query.class), eq(WorkerCallEntity.class))).thenReturn(Collections.emptyList());
            doCallRealMethod().when(supplementApiCallServer).callUpdate(infos);
            Assertions.assertDoesNotThrow(() -> supplementApiCallServer.callUpdate(infos));
            Assertions.assertNull(entity1.getP50());
            Assertions.assertNull(entity1.getP95());
            Assertions.assertNull(entity1.getP99());
            Assertions.assertNull(entity2.getP50());
            Assertions.assertNull(entity2.getP95());
            Assertions.assertNull(entity2.getP99());
        }
    }

    // Helper method for large dataset test
    private List<ApiCallDto> createLargeApiCallDtos(int count) {
        List<ApiCallDto> dtos = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            dtos.add(createApiCallDto("worker" + (i % 5), "api" + (i % 10), "gateway" + (i % 3),
                                    100L + i, "200", 60000L + i * 1000, 60100L + i * 1000));
        }
        return dtos;
    }
}
