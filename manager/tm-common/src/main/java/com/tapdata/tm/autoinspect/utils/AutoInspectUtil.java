package com.tapdata.tm.autoinspect.utils;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;
import com.tapdata.tm.autoinspect.entity.CheckAgainProgress;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/22 21:35 Create
 */
public class AutoInspectUtil {

    public static boolean isTimeout(CheckAgainProgress checkAgainProgress) {
        if (null != checkAgainProgress) {
            switch (checkAgainProgress.getStatus()) {
                case Running:
                case Scheduling:
                    if (System.currentTimeMillis() - checkAgainProgress.getUpdated().getTime() > AutoInspectConstants.CHECK_AGAIN_TIMEOUT) {
                        return true;
                    }
                    break;
                default:
                    break;
            }
        }
        return false;
    }

    public static AutoInspectProgress toAutoInspectProgress(Map<String, Object> taskAttrs) {
        if (null != taskAttrs) {
            Object progress = taskAttrs.get(AutoInspectConstants.AUTO_INSPECT_PROGRESS_KEY);
            if (progress instanceof AutoInspectProgress) {
                return (AutoInspectProgress) progress;
            } else if (progress instanceof Map) {
                String jsonStr = JSON.toJSONString(progress);
                return JSON.parseObject(jsonStr, AutoInspectProgress.class);
            }
        }
        return null;
    }

    public static CheckAgainProgress toCheckAgainProgress(Map<String, Object> taskAttrs) {
        if (null != taskAttrs) {
            Object progress = taskAttrs.get(AutoInspectConstants.CHECK_AGAIN_PROGRESS_KEY);
            if (progress instanceof CheckAgainProgress) {
                return (CheckAgainProgress) progress;
            } else if (progress instanceof Map) {
                String jsonStr = JSON.toJSONString(progress);
                return JSON.parseObject(jsonStr, CheckAgainProgress.class);
            }
        }
        return null;
    }

    /**
     * delete AutoInspect progress in task attrs
     *
     * @param taskAttrs task attributes on task.attrs
     */
    public static void removeProgress(Map<String, Object> taskAttrs) {
        if (null != taskAttrs) {
            taskAttrs.remove(AutoInspectConstants.AUTO_INSPECT_PROGRESS_KEY);
            taskAttrs.remove(AutoInspectConstants.CHECK_AGAIN_PROGRESS_KEY);
        }
    }

    /**
     * Batch number length 20 and may repeat within the same millisecond
     *
     * @return string(20)
     */
    public static String newBatchNumber() {
        return new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()) + (int) (Math.random() * 999);
    }
}
