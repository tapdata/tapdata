package com.tapdata.tm.utils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ResultMapUtil {
    public static Map<String,Object> resultMap(String testTaskId, boolean isSucceed, String message){
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("taskId", testTaskId);
        errorMap.put("ts", new Date().getTime());
        errorMap.put("code", isSucceed ? "ok" : "error");
        errorMap.put("message", message);
        return errorMap;
    }
}