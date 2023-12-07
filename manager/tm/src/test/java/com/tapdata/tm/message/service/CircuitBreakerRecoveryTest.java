package com.tapdata.tm.message.service;

import com.alibaba.fastjson.JSONObject;
import com.tapdata.tm.utils.HttpUtils;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CircuitBreakerRecoveryTest {

    @Mock
    private WorkerService mockWorkerService;

    @InjectMocks
    private CircuitBreakerRecoveryService circuitBreakerRecoveryUnderTest;



    @Test
    void testCheckServiceStatus() throws InterruptedException {
        long mockBeforeAvailableAgentCount = 100;
        long mockAvailableAgentCount = 100;
        try (MockedStatic<HttpUtils> httpUtilsMockedStatic = Mockito.mockStatic(HttpUtils.class)){
            Map<String,String> mockMap = new HashMap<>();
            String content = "熔断恢复，服务已恢复正常";
            mockMap.put("title", "服务熔断恢复提醒");
            mockMap.put("content", content);
            mockMap.put("color", "green");
            mockMap.put("groupId","oc_d6bc5fe48d56453264ec73a2fb3eec70");
            httpUtilsMockedStatic.when(() -> HttpUtils.sendPostData("test", JSONObject.toJSONString(mockMap))).thenReturn("");
            when(mockWorkerService.getAvailableAgentCount()).thenReturn(mockAvailableAgentCount);
            circuitBreakerRecoveryUnderTest.checkServiceStatus(mockBeforeAvailableAgentCount,"test");
            Thread.sleep(1000);
            Assertions.assertTrue(circuitBreakerRecoveryUnderTest.checkTask.get());
        }
    }

    @Test
    void testCheckServiceStatus_SendFail() throws InterruptedException {
        long mockBeforeAvailableAgentCount = 100;
        long mockAvailableAgentCount = 10;
        try (MockedStatic<HttpUtils> httpUtilsMockedStatic = Mockito.mockStatic(HttpUtils.class)){
            Map<String,String> mockMap = new HashMap<>();
            String content = "熔断恢复，服务已恢复正常";
            mockMap.put("title", "服务熔断恢复提醒");
            mockMap.put("content", content);
            mockMap.put("color", "green");
            mockMap.put("groupId","oc_d6bc5fe48d56453264ec73a2fb3eec70");
            httpUtilsMockedStatic.when(() -> HttpUtils.sendPostData("test", JSONObject.toJSONString(mockMap))).thenReturn("");
            when(mockWorkerService.getAvailableAgentCount()).thenReturn(mockAvailableAgentCount);
            circuitBreakerRecoveryUnderTest.checkServiceStatus(mockBeforeAvailableAgentCount,"test");
            Thread.sleep(1000);
            Assertions.assertFalse(circuitBreakerRecoveryUnderTest.checkTask.get());
        }
    }
    @Test
    void testCheckAvailableAgentCount(){
        long beforeAvailableAgentCount = 100;
        long mockAvailableAgentCount = 100;
        when(mockWorkerService.getAvailableAgentCount()).thenReturn(mockAvailableAgentCount);
        boolean result = circuitBreakerRecoveryUnderTest.checkAvailableAgentCount(beforeAvailableAgentCount);
        Assertions.assertTrue(result);
    }

    @Test
    void testCheckAvailableAgentCount_fail(){
        long beforeAvailableAgentCount = 100;
        long mockAvailableAgentCount = 10;
        when(mockWorkerService.getAvailableAgentCount()).thenReturn(mockAvailableAgentCount);
        boolean result = circuitBreakerRecoveryUnderTest.checkAvailableAgentCount(beforeAvailableAgentCount);
        Assertions.assertFalse(result);
    }

    @Test
    void testCheckAvailableAgentCount_customThreshold(){
        long beforeAvailableAgentCount = 100;
        long mockAvailableAgentCount = 10;
        System.setProperty("circuit_breaker_recovery_threshold","0.1");
        when(mockWorkerService.getAvailableAgentCount()).thenReturn(mockAvailableAgentCount);
        boolean result = circuitBreakerRecoveryUnderTest.checkAvailableAgentCount(beforeAvailableAgentCount);
        Assertions.assertTrue(result);
    }

    @Test
    void testCheckAvailableAgentCount_customThresholdMorethanOne(){
        long beforeAvailableAgentCount = 100;
        long mockAvailableAgentCount = 10;
        System.setProperty("circuit_breaker_recovery_threshold","2");
        when(mockWorkerService.getAvailableAgentCount()).thenReturn(mockAvailableAgentCount);
        boolean result = circuitBreakerRecoveryUnderTest.checkAvailableAgentCount(beforeAvailableAgentCount);
        Assertions.assertFalse(result);
    }

    @Test
    void testCheckAvailableAgentCount_customThresholdIsString(){
        long beforeAvailableAgentCount = 100;
        long mockAvailableAgentCount = 10;
        System.setProperty("circuit_breaker_recovery_threshold","ssss");
        when(mockWorkerService.getAvailableAgentCount()).thenReturn(mockAvailableAgentCount);
        boolean result = circuitBreakerRecoveryUnderTest.checkAvailableAgentCount(beforeAvailableAgentCount);
        Assertions.assertFalse(result);
    }

}
