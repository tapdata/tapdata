package com.tapdata.tm.cluster.service;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.*;
import com.tapdata.tm.cluster.params.OracleLogParserConfigParam;
import com.tapdata.tm.utils.HttpUtils;
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
 * Unit tests for OracleLogParserService
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 */
class OracleLogParserServiceTest {
    
    OracleLogParserService oracleLogParserService;
    RawServerStateService rawServerStateService;
    
    @BeforeEach
    void init() {
        oracleLogParserService = mock(OracleLogParserService.class);
        rawServerStateService = mock(RawServerStateService.class);
        ReflectionTestUtils.setField(oracleLogParserService, "rawServerStateService", rawServerStateService);
    }

    @Test
    void testConstructor() {
        OracleLogParserService service = new OracleLogParserService();
        assertNotNull(service);
    }
    
    @Nested
    class FindByServerIdTest {
        
        @Test
        void testServerIdIsNull() {
            when(oracleLogParserService.findByServerId(null)).thenCallRealMethod();
            BizException exception = assertThrows(BizException.class, () -> oracleLogParserService.findByServerId(null));
            assertEquals("oracle.log.parser.server.id.empty", exception.getErrorCode());
        }
        
        @Test
        void testServerIdIsBlank() {
            when(oracleLogParserService.findByServerId("")).thenCallRealMethod();
            BizException exception = assertThrows(BizException.class, () -> oracleLogParserService.findByServerId(""));
            assertEquals("oracle.log.parser.server.id.empty", exception.getErrorCode());
        }
        
        @Test
        void testServerNotFound() {
            String serverId = "server123";
            Page<RawServerStateDto> emptyPage = new Page<>(0, new ArrayList<>());
            when(rawServerStateService.getAllLatest(any(Filter.class))).thenReturn(emptyPage);
            when(oracleLogParserService.findByServerId(serverId)).thenCallRealMethod();
            
            BizException exception = assertThrows(BizException.class, () -> oracleLogParserService.findByServerId(serverId));
            assertEquals("oracle.log.parser.unable.find.server", exception.getErrorCode());
        }
        
        @Test
        void testServerFoundButFirstItemIsNull() {
            String serverId = "server123";
            List<RawServerStateDto> items = new ArrayList<>();
            items.add(null);
            Page<RawServerStateDto> page = new Page<>(1, items);
            when(rawServerStateService.getAllLatest(any(Filter.class))).thenReturn(page);
            when(oracleLogParserService.findByServerId(serverId)).thenCallRealMethod();
            
            BizException exception = assertThrows(BizException.class, () -> oracleLogParserService.findByServerId(serverId));
            assertEquals("oracle.log.parser.unable.find.server", exception.getErrorCode());
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
            when(oracleLogParserService.findByServerId(serverId)).thenCallRealMethod();
            
            BizException exception = assertThrows(BizException.class, () -> oracleLogParserService.findByServerId(serverId));
            assertEquals("oracle.log.parser.unable.invalid.server", exception.getErrorCode());
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
            when(oracleLogParserService.findByServerId(serverId)).thenCallRealMethod();
            
            BizException exception = assertThrows(BizException.class, () -> oracleLogParserService.findByServerId(serverId));
            assertEquals("oracle.log.parser.unable.invalid.server", exception.getErrorCode());
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
            when(oracleLogParserService.findByServerId(serverId)).thenCallRealMethod();
            
            RawServerStateDto result = oracleLogParserService.findByServerId(serverId);
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
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            OracleLogParserCommandExecResult expectedResult = new OracleLogParserCommandExecResult();
            expectedResult.setStatus("success");
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserCommandExecResult.class))).thenReturn(expectedResult);
            when(oracleLogParserService.executeCommand(serverId, "start")).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(OracleLogParserCommandExecResult.class))).thenReturn(expectedResult);

                OracleLogParserCommandExecResult result = oracleLogParserService.executeCommand(serverId, "start");
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testExecuteStopCommand() {
            String serverId = "server123";
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            OracleLogParserCommandExecResult expectedResult = new OracleLogParserCommandExecResult();
            expectedResult.setStatus("success");
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserCommandExecResult.class))).thenReturn(expectedResult);
            when(oracleLogParserService.executeCommand(serverId, "stop")).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(OracleLogParserCommandExecResult.class))).thenReturn(expectedResult);

                OracleLogParserCommandExecResult result = oracleLogParserService.executeCommand(serverId, "stop");
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testExecuteRestartCommand() {
            String serverId = "server123";
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            OracleLogParserCommandExecResult expectedResult = new OracleLogParserCommandExecResult();
            expectedResult.setStatus("success");
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserCommandExecResult.class))).thenReturn(expectedResult);
            when(oracleLogParserService.executeCommand(serverId, "restart")).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(OracleLogParserCommandExecResult.class))).thenReturn(expectedResult);

                OracleLogParserCommandExecResult result = oracleLogParserService.executeCommand(serverId, "restart");
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testExecuteInvalidCommand() {
            String serverId = "server123";
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            when(oracleLogParserService.executeCommand(serverId, "invalid")).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.executeCommand(serverId, "invalid"));
            assertEquals("oracle.log.parser.unable.invalid.command", exception.getErrorCode());
        }

        @Test
        void testExecuteCommandWithException() {
            String serverId = "server123";
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserCommandExecResult.class)))
                .thenThrow(new RuntimeException("Network error"));
            when(oracleLogParserService.executeCommand(serverId, "start")).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.executeCommand(serverId, "start"));
            assertEquals("oracle.log.parser.command.failed", exception.getErrorCode());
        }
    }

    @Nested
    class UpdateOracleLogParserConfigTest {

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
            OracleLogParserConfigParam param = new OracleLogParserConfigParam();
            param.setOracleUrl("");
            param.setMapTable("");
            when(oracleLogParserService.updateOracleLogParserConfig(serverId, param)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.updateOracleLogParserConfig(serverId, param));
            assertEquals("oracle.log.parser.update.config.cannot.be.empty", exception.getErrorCode());
        }

        @Test
        void testUpdateConfigWithBothParamsNull() {
            String serverId = "server123";
            OracleLogParserConfigParam param = new OracleLogParserConfigParam();
            when(oracleLogParserService.updateOracleLogParserConfig(serverId, param)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.updateOracleLogParserConfig(serverId, param));
            assertEquals("oracle.log.parser.update.config.cannot.be.empty", exception.getErrorCode());
        }

        @Test
        void testUpdateConfigWithOracleUrlOnly() {
            String serverId = "server123";
            OracleLogParserConfigParam param = new OracleLogParserConfigParam();
            param.setOracleUrl("jdbc:oracle:thin:@localhost:1521:orcl");
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            OracleLogParserUpdateConfigResult expectedResult = new OracleLogParserUpdateConfigResult();
            expectedResult.setStatus("success");
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserUpdateConfigResult.class))).thenReturn(expectedResult);
            when(oracleLogParserService.updateOracleLogParserConfig(serverId, param)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(OracleLogParserUpdateConfigResult.class))).thenReturn(expectedResult);

                OracleLogParserUpdateConfigResult result = oracleLogParserService.updateOracleLogParserConfig(serverId, param);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testUpdateConfigWithMapTableOnly() {
            String serverId = "server123";
            OracleLogParserConfigParam param = new OracleLogParserConfigParam();
            param.setMapTable("table_mapping");
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            OracleLogParserUpdateConfigResult expectedResult = new OracleLogParserUpdateConfigResult();
            expectedResult.setStatus("success");
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserUpdateConfigResult.class))).thenReturn(expectedResult);
            when(oracleLogParserService.updateOracleLogParserConfig(serverId, param)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(OracleLogParserUpdateConfigResult.class))).thenReturn(expectedResult);

                OracleLogParserUpdateConfigResult result = oracleLogParserService.updateOracleLogParserConfig(serverId, param);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testUpdateConfigWithBothParams() {
            String serverId = "server123";
            OracleLogParserConfigParam param = new OracleLogParserConfigParam();
            param.setOracleUrl("jdbc:oracle:thin:@localhost:1521:orcl");
            param.setMapTable("table_mapping");
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            OracleLogParserUpdateConfigResult expectedResult = new OracleLogParserUpdateConfigResult();
            expectedResult.setStatus("success");
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserUpdateConfigResult.class))).thenReturn(expectedResult);
            when(oracleLogParserService.updateOracleLogParserConfig(serverId, param)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(OracleLogParserUpdateConfigResult.class))).thenReturn(expectedResult);

                OracleLogParserUpdateConfigResult result = oracleLogParserService.updateOracleLogParserConfig(serverId, param);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testUpdateConfigWithException() {
            String serverId = "server123";
            OracleLogParserConfigParam param = new OracleLogParserConfigParam();
            param.setOracleUrl("jdbc:oracle:thin:@localhost:1521:orcl");
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserUpdateConfigResult.class)))
                .thenThrow(new RuntimeException("Network error"));
            when(oracleLogParserService.updateOracleLogParserConfig(serverId, param)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.updateOracleLogParserConfig(serverId, param));
            assertEquals("oracle.log.parser.update.config.failed", exception.getErrorCode());
        }
    }

    @Nested
    class UpgradeOracleLogParserSNTest {

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
            when(oracleLogParserService.upgradeOracleLogParserSN(serverId, null)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.upgradeOracleLogParserSN(serverId, null));
            assertEquals("oracle.log.parser.sn.file.empty", exception.getErrorCode());
        }

        @Test
        void testUpgradeSNWithBlankContext() {
            String serverId = "server123";
            when(oracleLogParserService.upgradeOracleLogParserSN(serverId, "")).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.upgradeOracleLogParserSN(serverId, ""));
            assertEquals("oracle.log.parser.sn.file.empty", exception.getErrorCode());
        }

        @Test
        void testUpgradeSNSuccessfully() {
            String serverId = "server123";
            String snContext = "SN_CONTEXT_12345";
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            OracleLogParserUpgradeSNResult expectedResult = new OracleLogParserUpgradeSNResult();
            expectedResult.setStatus("success");
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserUpgradeSNResult.class))).thenReturn(expectedResult);
            when(oracleLogParserService.upgradeOracleLogParserSN(serverId, snContext)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendPostData(anyString(), anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.toJSONString(any())).thenReturn("{}");
                json.when(() -> JSON.parseObject(anyString(), eq(OracleLogParserUpgradeSNResult.class))).thenReturn(expectedResult);

                OracleLogParserUpgradeSNResult result = oracleLogParserService.upgradeOracleLogParserSN(serverId, snContext);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }

        @Test
        void testUpgradeSNWithException() {
            String serverId = "server123";
            String snContext = "SN_CONTEXT_12345";
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            when(oracleLogParserService.post(anyString(), any(), eq(OracleLogParserUpgradeSNResult.class)))
                .thenThrow(new RuntimeException("Network error"));
            when(oracleLogParserService.upgradeOracleLogParserSN(serverId, snContext)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.upgradeOracleLogParserSN(serverId, snContext));
            assertEquals("oracle.log.parser.update.sn.failed", exception.getErrorCode());
        }
    }

    @Nested
    class FindOracleLogParserSNTest {

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
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);

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

            when(oracleLogParserService.get(anyString(), eq(Map.class))).thenReturn(responseMap);
            when(oracleLogParserService.findOracleLogParserSN(serverId)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendGetData(anyString(), any(), anyBoolean(), anyBoolean()))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.parseObject(anyString(), eq(Map.class))).thenReturn(responseMap);

                OracleLogParserSNResult result = oracleLogParserService.findOracleLogParserSN(serverId);
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
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            when(oracleLogParserService.get(anyString(), eq(Map.class)))
                .thenThrow(new RuntimeException("Network error"));
            when(oracleLogParserService.findOracleLogParserSN(serverId)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.findOracleLogParserSN(serverId));
            assertEquals("oracle.log.parser.find.sn.failed", exception.getErrorCode());
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
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            doNothing().when(rawServerStateService).deleteAll(serverId);
            when(oracleLogParserService.removeUselessServerInfo(serverId)).thenCallRealMethod();

            Boolean result = oracleLogParserService.removeUselessServerInfo(serverId);
            assertTrue(result);
            verify(rawServerStateService, times(1)).deleteAll(serverId);
        }

        @Test
        void testRemoveServerWithException() {
            String serverId = "server123";
            when(oracleLogParserService.findByServerId(serverId)).thenReturn(mockServer);
            doThrow(new RuntimeException("Database error")).when(rawServerStateService).deleteAll(serverId);
            when(oracleLogParserService.removeUselessServerInfo(serverId)).thenCallRealMethod();

            BizException exception = assertThrows(BizException.class,
                () -> oracleLogParserService.removeUselessServerInfo(serverId));
            assertEquals("oracle.log.parser.remove.server.failed", exception.getErrorCode());
        }
    }

    @Nested
    class GetAndPostMethodsTest {

        @Test
        void testGetMethod() {
            String url = "http://192.168.1.1:8080/api/test";
            Map<String, Object> expectedResult = new HashMap<>();
            expectedResult.put("key", "value");
            when(oracleLogParserService.get(url, Map.class)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                httpUtils.when(() -> HttpUtils.sendGetData(eq(url), any(), eq(false), eq(false)))
                        .thenReturn("{\"key\":\"value\"}");
                json.when(() -> JSON.parseObject("{\"key\":\"value\"}", Map.class)).thenReturn(expectedResult);

                Map<String, Object> result = oracleLogParserService.get(url, Map.class);
                assertNotNull(result);
                assertEquals("value", result.get("key"));
            }
        }

        @Test
        void testPostMethod() {
            String url = "http://192.168.1.1:8080/api/test";
            Map<String, Object> body = new HashMap<>();
            body.put("param", "value");
            OracleLogParserCommandExecResult expectedResult = new OracleLogParserCommandExecResult();
            expectedResult.setStatus("success");
            when(oracleLogParserService.post(url, body, OracleLogParserCommandExecResult.class)).thenCallRealMethod();

            try (MockedStatic<HttpUtils> httpUtils = mockStatic(HttpUtils.class);
                 MockedStatic<JSON> json = mockStatic(JSON.class)) {
                json.when(() -> JSON.toJSONString(body)).thenReturn("{\"param\":\"value\"}");
                httpUtils.when(() -> HttpUtils.sendPostData(eq(url), eq("{\"param\":\"value\"}"), any(), eq(false), eq(false)))
                        .thenReturn("{\"status\":\"success\"}");
                json.when(() -> JSON.parseObject("{\"status\":\"success\"}", OracleLogParserCommandExecResult.class))
                        .thenReturn(expectedResult);

                OracleLogParserCommandExecResult result = oracleLogParserService.post(url, body, OracleLogParserCommandExecResult.class);
                assertNotNull(result);
                assertEquals("success", result.getStatus());
            }
        }
    }
}

