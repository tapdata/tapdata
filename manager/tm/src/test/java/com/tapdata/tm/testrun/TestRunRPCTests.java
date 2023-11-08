package com.tapdata.tm.testrun;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.tm.commons.dag.vo.TestRunDto;
import com.tapdata.tm.task.controller.TaskController;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TestRunRPCTests {
    private static TaskController taskController;
    private static TestRunDto testRunDto;
    private static Map<String, Object> result;

    @BeforeEach
    void beforeAll(){
        taskController = new TaskController();
        testRunDto = new TestRunDto();
        result = new HashMap<>();
        testRunDto.setJsNodeId("0495bffa-62c7-4d66-a0f8-8e20693a189f");
        JSONArray logs = new JSONArray();
        JSONObject logInfo1 = new JSONObject();
        logInfo1.put("level","INFO");
        logInfo1.put("nodeId","0495bffa-62c7-4d66-a0f8-8e20693a1899");
        logs.add(logInfo1);
        JSONObject logInfo2 = new JSONObject();
        logInfo2.put("level","WARN");
        logInfo2.put("nodeId","0495bffa-62c7-4d66-a0f8-8e20693a1899");
        logs.add(logInfo2);
        JSONObject logInfo3 = new JSONObject();
        logInfo3.put("level","INFO");
        logInfo3.put("nodeId","0495bffa-62c7-4d66-a0f8-8e20693a189f");
        logs.add(logInfo3);
        result.put("logs",logs);
    }
    @Test
    public void testBuildFilteredLogs(){
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(2,filteredLogs.size());
    }

    @Test
    void testBuildFilteredLogsTestRunDtoIsNull(){
        JSONArray filteredLogs = taskController.buildFilteredLogs(null, result);
        Assertions.assertEquals(1,filteredLogs.size());
    }

    @Test
    void testBuildFilteredLogsResultIsNull(){
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, null);
        Assertions.assertEquals(0,filteredLogs.size());
    }

    @Test
    void testBuildFilteredLogsResultWithoutLevel(){
        JSONObject logInfo = new JSONObject();
        logInfo.put("nodeId","0495bffa-62c7-4d66-a0f8-8e20693a189f");
        ((JSONArray)result.get("logs")).add(logInfo);
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(2,filteredLogs.size());
    }

    @Test
    void testBuildFilteredLogsResultWithoutNodeId(){
        JSONObject logInfo = new JSONObject();
        logInfo.put("level","INFO");
        ((JSONArray)result.get("logs")).add(logInfo);
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(2,filteredLogs.size());
    }

    @Test
    void testBuildFilteredLogsResultWithoutLogs(){
        result.remove("logs");
        result.put("test","test");
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(0,filteredLogs.size());
    }
    @Test
    void testBuildFilteredLogsWithLevelLowerOrHasSpace(){
        JSONObject logInfo1 = new JSONObject();
        logInfo1.put("level","info");
        logInfo1.put("nodeId","0495bffa-62c7-4d66-a0f8-8e20693a1899");
        ((JSONArray)result.get("logs")).add(logInfo1);
        JSONObject logInfo2 = new JSONObject();
        logInfo2.put("level"," WARN ");
        logInfo2.put("nodeId","0495bffa-62c7-4d66-a0f8-8e20693a1899");
        ((JSONArray)result.get("logs")).add(logInfo2);
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(2,filteredLogs.size());
    }

    @Test
    void testBuildFilteredLogsJsNodeWithLevelLower(){
        JSONObject logInfo1 = new JSONObject();
        logInfo1.put("level","info");
        logInfo1.put("nodeId","0495bffa-62c7-4d66-a0f8-8e20693a189f");
        ((JSONArray)result.get("logs")).add(logInfo1);
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(3,filteredLogs.size());
    }
    @Test
    void testBuildFilteredLogsWithJsNodeIdIsNull(){
        testRunDto.setJsNodeId(null);
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(1,filteredLogs.size());
    }
    @Test
    void testBuildFilteredNodeIdIsEmpty(){
        JSONObject logInfo1 = new JSONObject();
        logInfo1.put("level","ERROR");
        logInfo1.put("nodeId","");
        ((JSONArray)result.get("logs")).add(logInfo1);
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(3,filteredLogs.size());
    }
    @Test
    void testBuildFilteredLogsIsNotJsonArray(){
        result.put("logs","logInfo");
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(0,filteredLogs.size());
    }
    @Test
    void testBuildFilteredLogIsNotJsonObject(){
        String logInfo = "test";
        ((JSONArray)result.get("logs")).add(logInfo);
        JSONArray filteredLogs = taskController.buildFilteredLogs(testRunDto, result);
        Assertions.assertEquals(2,filteredLogs.size());
    }
}
