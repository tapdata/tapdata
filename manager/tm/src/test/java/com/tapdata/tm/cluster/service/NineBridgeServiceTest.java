package com.tapdata.tm.cluster.service;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.*;
import com.tapdata.tm.cluster.params.NineBridgeConfigParam;
import com.tapdata.tm.utils.HttpUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for NineBridgeService
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 */
class NineBridgeServiceTest {
    
    NineBridgeService nineBridgeService;
    RawServerStateService rawServerStateService;
    
    @BeforeEach
    void init() {
        nineBridgeService = mock(NineBridgeService.class);
        rawServerStateService = mock(RawServerStateService.class);
        ReflectionTestUtils.setField(nineBridgeService, "rawServerStateService", rawServerStateService);
    }
    
    @Nested
    class FindByServerIdTest {
        
        @Test
        void testServerIdIsNull() {
            when(nineBridgeService.findByServerId(null)).thenCallRealMethod();
            BizException exception = assertThrows(BizException.class, () -> nineBridgeService.findByServerId(null));
            assertEquals("nine.bridge.server.id.empty", exception.getErrorCode());
        }
        
        @Test
        void testServerIdIsBlank() {
            when(nineBridgeService.findByServerId("")).thenCallRealMethod();
            BizException exception = assertThrows(BizException.class, () -> nineBridgeService.findByServerId(""));
            assertEquals("nine.bridge.server.id.empty", exception.getErrorCode());
        }
        
        @Test
        void testServerNotFound() {
            String serverId = "server123";
            Page<RawServerStateDto> emptyPage = new Page<>(0, new ArrayList<>());
            when(rawServerStateService.getAllLatest(any(Filter.class))).thenReturn(emptyPage);
            when(nineBridgeService.findByServerId(serverId)).thenCallRealMethod();
            
            BizException exception = assertThrows(BizException.class, () -> nineBridgeService.findByServerId(serverId));
            assertEquals("nine.bridge.unable.find.server", exception.getErrorCode());
        }
        
        @Test
        void testServerFoundButFirstItemIsNull() {
            String serverId = "server123";
            List<RawServerStateDto> items = new ArrayList<>();
            items.add(null);
            Page<RawServerStateDto> page = new Page<>(1, items);
            when(rawServerStateService.getAllLatest(any(Filter.class))).thenReturn(page);
            when(nineBridgeService.findByServerId(serverId)).thenCallRealMethod();
            
            BizException exception = assertThrows(BizException.class, () -> nineBridgeService.findByServerId(serverId));
            assertEquals("nine.bridge.unable.find.server", exception.getErrorCode());
        }
        
        @Test
        void testServerFoundButServiceIPIsBlank() {
            String serverId = "server123";
            RawServerStateDto server = new RawServerStateDto();
            server.setServiceIP("");
            server.setServicePort(8080);
            List<RawServerStateDto> items = new ArrayList<>();
            items.add(server);
            Page<RawServerStateDto> page = new Page<>(1, items);
            when(rawServerStateService.getAllLatest(any(Filter.class))).thenReturn(page);
            when(nineBridgeService.findByServerId(serverId)).thenCallRealMethod();
            
            BizException exception = assertThrows(BizException.class, () -> nineBridgeService.findByServerId(serverId));
            assertEquals("nine.bridge.unable.invalid.server", exception.getErrorCode());
        }
        
        @Test
        void testServerFoundButServicePortIsNull() {
            String serverId = "server123";
            RawServerStateDto server = new RawServerStateDto();
            server.setServiceIP("192.168.1.1");
            server.setServicePort(null);
            List<RawServerStateDto> items = new ArrayList<>();
            items.add(server);
            Page<RawServerStateDto> page = new Page<>(1, items);
            when(rawServerStateService.getAllLatest(any(Filter.class))).thenReturn(page);
            when(nineBridgeService.findByServerId(serverId)).thenCallRealMethod();
            
            BizException exception = assertThrows(BizException.class, () -> nineBridgeService.findByServerId(serverId));
            assertEquals("nine.bridge.unable.invalid.server", exception.getErrorCode());
        }
        
        @Test
        void testServerFoundSuccessfully() {
            String serverId = "server123";
            RawServerStateDto server = new RawServerStateDto();
            server.setServiceIP("192.168.1.1");
            server.setServicePort(8080);
            List<RawServerStateDto> items = new ArrayList<>();
            items.add(server);
            Page<RawServerStateDto> page = new Page<>(1, items);
            when(rawServerStateService.getAllLatest(any(Filter.class))).thenReturn(page);
            when(nineBridgeService.findByServerId(serverId)).thenCallRealMethod();
            
            RawServerStateDto result = nineBridgeService.findByServerId(serverId);
            assertNotNull(result);
            assertEquals("192.168.1.1", result.getServiceIP());
            assertEquals(8080, result.getServicePort());
        }
    }
    
    @Nested
    class ExecuteCommandTest {
        
        RawServerStateDto mockServer;
        
        @BeforeEach
        void setup() {
            mockServer = new RawServerStateDto();
            mockServer.setServiceIP("192.168.1.1");
            mockServer.setServicePort(8080);
        }
        
        @Test
        void testExecuteStartCommand() {
            String serverId = "server123";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            NineBridgeCommandExecResult expectedResult = new NineBridgeCommandExecResult();
            expectedResult.setStatus("success");
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeCommandExecResult.class))).thenReturn(expectedResult);
            when(nineBridgeService.executeCommand(serverId, "start")).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(NineBridgeCommandExecResult.class))).thenReturn(expectedResult);

                NineBridgeCommandExecResult result = nineBridgeService.executeCommand(serverId, "start");
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testExecuteStopCommand() {
            String serverId = "server123";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            NineBridgeCommandExecResult expectedResult = new NineBridgeCommandExecResult();
            expectedResult.setStatus("success");
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeCommandExecResult.class))).thenReturn(expectedResult);
            when(nineBridgeService.executeCommand(serverId, "stop")).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(NineBridgeCommandExecResult.class))).thenReturn(expectedResult);

                NineBridgeCommandExecResult result = nineBridgeService.executeCommand(serverId, "stop");
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testExecuteRestartCommand() {
            String serverId = "server123";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            NineBridgeCommandExecResult expectedResult = new NineBridgeCommandExecResult();
            expectedResult.setStatus("success");
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeCommandExecResult.class))).thenReturn(expectedResult);
            when(nineBridgeService.executeCommand(serverId, "restart")).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(NineBridgeCommandExecResult.class))).thenReturn(expectedResult);

                NineBridgeCommandExecResult result = nineBridgeService.executeCommand(serverId, "restart");
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testExecuteInvalidCommand() {
            String serverId = "server123";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            when(nineBridgeService.executeCommand(serverId, "invalid")).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.executeCommand(serverId, "invalid"));
            assertEquals("nine.bridge.unable.invalid.command", exception.getErrorCode());
        }

        @Test
        void testExecuteCommandWithException() {
            String serverId = "server123";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeCommandExecResult.class)))
                .thenThrow(new RuntimeException("Network error"));
            when(nineBridgeService.executeCommand(serverId, "start")).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.executeCommand(serverId, "start"));
            assertEquals("nine.bridge.command.failed", exception.getErrorCode());
        }
    }

    @Nested
    class UpdateNineBridgeConfigTest {

        RawServerStateDto mockServer;

        @BeforeEach
        void setup() {
            mockServer = new RawServerStateDto();
            mockServer.setServiceIP("192.168.1.1");
            mockServer.setServicePort(8080);
        }

        @Test
        void testUpdateConfigWithBothParamsEmpty() {
            String serverId = "server123";
            NineBridgeConfigParam param = new NineBridgeConfigParam();
            param.setOracleUrl("");
            param.setMapTable("");
            when(nineBridgeService.updateNineBridgeConfig(serverId, param)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.updateNineBridgeConfig(serverId, param));
            assertEquals("nine.bridge.update.config.cannot.be.empty", exception.getErrorCode());
        }

        @Test
        void testUpdateConfigWithBothParamsNull() {
            String serverId = "server123";
            NineBridgeConfigParam param = new NineBridgeConfigParam();
            when(nineBridgeService.updateNineBridgeConfig(serverId, param)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.updateNineBridgeConfig(serverId, param));
            assertEquals("nine.bridge.update.config.cannot.be.empty", exception.getErrorCode());
        }

        @Test
        void testUpdateConfigWithOracleUrlOnly() {
            String serverId = "server123";
            NineBridgeConfigParam param = new NineBridgeConfigParam();
            param.setOracleUrl("jdbc:oracle:thin:@localhost:1521:orcl");
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            NineBridgeUpdateConfigResult expectedResult = new NineBridgeUpdateConfigResult();
            expectedResult.setStatus("success");
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeUpdateConfigResult.class))).thenReturn(expectedResult);
            when(nineBridgeService.updateNineBridgeConfig(serverId, param)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(NineBridgeUpdateConfigResult.class))).thenReturn(expectedResult);

                NineBridgeUpdateConfigResult result = nineBridgeService.updateNineBridgeConfig(serverId, param);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testUpdateConfigWithMapTableOnly() {
            String serverId = "server123";
            NineBridgeConfigParam param = new NineBridgeConfigParam();
            param.setMapTable("table_mapping");
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            NineBridgeUpdateConfigResult expectedResult = new NineBridgeUpdateConfigResult();
            expectedResult.setStatus("success");
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeUpdateConfigResult.class))).thenReturn(expectedResult);
            when(nineBridgeService.updateNineBridgeConfig(serverId, param)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(NineBridgeUpdateConfigResult.class))).thenReturn(expectedResult);

                NineBridgeUpdateConfigResult result = nineBridgeService.updateNineBridgeConfig(serverId, param);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testUpdateConfigWithBothParams() {
            String serverId = "server123";
            NineBridgeConfigParam param = new NineBridgeConfigParam();
            param.setOracleUrl("jdbc:oracle:thin:@localhost:1521:orcl");
            param.setMapTable("table_mapping");
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            NineBridgeUpdateConfigResult expectedResult = new NineBridgeUpdateConfigResult();
            expectedResult.setStatus("success");
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeUpdateConfigResult.class))).thenReturn(expectedResult);
            when(nineBridgeService.updateNineBridgeConfig(serverId, param)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(NineBridgeUpdateConfigResult.class))).thenReturn(expectedResult);

                NineBridgeUpdateConfigResult result = nineBridgeService.updateNineBridgeConfig(serverId, param);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testUpdateConfigWithException() {
            String serverId = "server123";
            NineBridgeConfigParam param = new NineBridgeConfigParam();
            param.setOracleUrl("jdbc:oracle:thin:@localhost:1521:orcl");
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeUpdateConfigResult.class)))
                .thenThrow(new RuntimeException("Network error"));
            when(nineBridgeService.updateNineBridgeConfig(serverId, param)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.updateNineBridgeConfig(serverId, param));
            assertEquals("nine.bridge.update.config.failed", exception.getErrorCode());
        }
    }

    @Nested
    class UpgradeNineBridgeSNTest {

        RawServerStateDto mockServer;

        @BeforeEach
        void setup() {
            mockServer = new RawServerStateDto();
            mockServer.setServiceIP("192.168.1.1");
            mockServer.setServicePort(8080);
        }

        @Test
        void testUpgradeSNWithNullContext() {
            String serverId = "server123";
            when(nineBridgeService.upgradeNineBridgeSN(serverId, null)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.upgradeNineBridgeSN(serverId, null));
            assertEquals("nine.bridge.sn.file.empty", exception.getErrorCode());
        }

        @Test
        void testUpgradeSNWithBlankContext() {
            String serverId = "server123";
            when(nineBridgeService.upgradeNineBridgeSN(serverId, "")).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.upgradeNineBridgeSN(serverId, ""));
            assertEquals("nine.bridge.sn.file.empty", exception.getErrorCode());
        }

        @Test
        void testUpgradeSNSuccessfully() {
            String serverId = "server123";
            String snContext = "SN_CONTEXT_12345";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            NineBridgeUpgradeSNResult expectedResult = new NineBridgeUpgradeSNResult();
            expectedResult.setStatus("success");
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeUpgradeSNResult.class))).thenReturn(expectedResult);
            when(nineBridgeService.upgradeNineBridgeSN(serverId, snContext)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(NineBridgeUpgradeSNResult.class))).thenReturn(expectedResult);

                NineBridgeUpgradeSNResult result = nineBridgeService.upgradeNineBridgeSN(serverId, snContext);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testUpgradeSNWithException() {
            String serverId = "server123";
            String snContext = "SN_CONTEXT_12345";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            when(nineBridgeService.post(anyString(), any(), eq(NineBridgeUpgradeSNResult.class)))
                .thenThrow(new RuntimeException("Network error"));
            when(nineBridgeService.upgradeNineBridgeSN(serverId, snContext)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.upgradeNineBridgeSN(serverId, snContext));
            assertEquals("nine.bridge.update.sn.failed", exception.getErrorCode());
        }
    }

    @Nested
    class FindNineBridgeSNTest {

        RawServerStateDto mockServer;

        @BeforeEach
        void setup() {
            mockServer = new RawServerStateDto();
            mockServer.setServiceIP("192.168.1.1");
            mockServer.setServicePort(8080);
        }

        @Test
        void testFindSNSuccessfully() {
            String serverId = "server123";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);

            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("status", "success");
            responseMap.put("message", "OK");
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("days", 365);
            dataMap.put("issueDate", "2024-01-01");
            dataMap.put("modules", 10);
            dataMap.put("nHosts", 5);
            dataMap.put("oraVersion", "19c");
            dataMap.put("platForm", "Linux");
            dataMap.put("serverId", "server123");
            dataMap.put("timestamp", 1234567890L);
            dataMap.put("user", "admin");
            responseMap.put("data", dataMap);

            when(nineBridgeService.get(anyString(), eq(Map.class))).thenReturn(responseMap);
            when(nineBridgeService.findNineBridgeSN(serverId)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendGetData(anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.parseObject(anyString(), eq(Map.class))).thenReturn(responseMap);

                NineBridgeSNResult result = nineBridgeService.findNineBridgeSN(serverId);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
                assertEquals("OK", result.getMessage());
                assertNotNull(result.getData());
                assertEquals(365, result.getData().getDays());
                assertEquals("2024-01-01", result.getData().getIssueDate());
            }
        }

        @Test
        void testFindSNWithException() {
            String serverId = "server123";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            when(nineBridgeService.get(anyString(), eq(Map.class)))
                .thenThrow(new RuntimeException("Network error"));
            when(nineBridgeService.findNineBridgeSN(serverId)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.findNineBridgeSN(serverId));
            assertEquals("nine.bridge.find.sn.failed", exception.getErrorCode());
        }
    }

    @Nested
    class RemoveUselessServerInfoTest {

        RawServerStateDto mockServer;

        @BeforeEach
        void setup() {
            mockServer = new RawServerStateDto();
            mockServer.setServiceIP("192.168.1.1");
            mockServer.setServicePort(8080);
        }

        @Test
        void testRemoveServerSuccessfully() {
            String serverId = "server123";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            doNothing().when(rawServerStateService).deleteAll(serverId);
            when(nineBridgeService.removeUselessServerInfo(serverId)).thenCallRealMethod();

            Boolean result = nineBridgeService.removeUselessServerInfo(serverId);
            assertTrue(result);
            verify(rawServerStateService, times(1)).deleteAll(serverId);
        }

        @Test
        void testRemoveServerWithException() {
            String serverId = "server123";
            when(nineBridgeService.findByServerId(serverId)).thenReturn(mockServer);
            doThrow(new RuntimeException("Database error")).when(rawServerStateService).deleteAll(serverId);
            when(nineBridgeService.removeUselessServerInfo(serverId)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> nineBridgeService.removeUselessServerInfo(serverId));
            assertEquals("nine.bridge.remove.server.failed", exception.getErrorCode());
        }
    }

    @Nested
    class GetAndPostMethodsTest {

        @Test
        void testGetMethod() {
            String url = "http://192.168.1.1:8080/api/test";
            Map<String, Object> expectedResult = new HashMap<>();
            expectedResult.put("key", "value");
            when(nineBridgeService.get(url, Map.class)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendGetData(eq(url), any(), eq(false), eq(false)))
                        .thenReturn("{\"key\":\"value\"}");
                json.when(() -> JSON.parseObject("{\"key\":\"value\"}", Map.class)).thenReturn(expectedResult);

                Map<String, Object> result = nineBridgeService.get(url, Map.class);
                assertNotNull(result);
                assertEquals("value", result.get("key"));
            }
        }

        @Test
        void testPostMethod() {
            String url = "http://192.168.1.1:8080/api/test";
            Map<String, Object> body = new HashMap<>();
            body.put("param", "value");
            NineBridgeCommandExecResult expectedResult = new NineBridgeCommandExecResult();
            expectedResult.setStatus("success");
            when(nineBridgeService.post(url, body, NineBridgeCommandExecResult.class)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                json.when(() -> JSON.toJSONString(body)).thenReturn("{\"param\":\"value\"}");
                httpUtils.when(() -> HttpUtils.sendPostData(eq(url), eq("{\"param\":\"value\"}"), any(), eq(false), eq(false)))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.parseObject("{\"status\":\"success\"}", NineBridgeCommandExecResult.class))
                        .thenReturn(expectedResult);

                NineBridgeCommandExecResult result = nineBridgeService.post(url, body, NineBridgeCommandExecResult.class);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }
    }
}

