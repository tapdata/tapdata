package com.tapdata.tm.autoinspect.utils;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;

import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/22 21:35 Create
 */
public class AutoInspectUtil {

    public static AutoInspectProgress parse(Map<String, Object> taskAttrs) {
        if (null != taskAttrs) {
            Object autoInspectProgress = taskAttrs.get("autoInspectProgress");
            if (autoInspectProgress instanceof Map) {
                String jsonStr = JSON.toJSONString(autoInspectProgress);
                return JSON.parseObject(jsonStr, AutoInspectProgress.class);
            }
        }
        return null;
    }
}
