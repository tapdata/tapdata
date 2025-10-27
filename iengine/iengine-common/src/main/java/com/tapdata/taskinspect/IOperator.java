package com.tapdata.taskinspect;

import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.cons.JobTypeEnum;
import com.tapdata.tm.taskinspect.dto.TaskInspectHistoriesDto;
import com.tapdata.tm.taskinspect.vo.CheckItemInfo;
import com.tapdata.tm.taskinspect.vo.JobReportVo;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 任务内校验-对接 TM 的操作接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/14 15:17 Create
 */
public interface IOperator {

    default TaskInspectConfig getConfig(String taskId) {
        return null;
    }

    default TaskInspectHistoriesDto jobStart(String taskId, JobTypeEnum type, Serializable config, Map<String, Serializable> ext) {
        return null;
    }

    default boolean postJobStatus(String jobId, JobReportVo vo) {
        return false;
    }

    default LinkedHashMap<String, Serializable> params(String key, Serializable value) {
        LinkedHashMap<String, Serializable> params = new LinkedHashMap<>();
        params.put(key, value);
        return params;
    }

    default LinkedHashMap<String, Serializable> params(LinkedHashMap<String, Serializable> params, String key, Serializable value) {
        params.put(key, value);
        return params;
    }

    default void recoverResult(String taskId, LinkedHashMap<String, Object> reportData) {
    }

    default void reportResult(String taskId, LinkedHashMap<String, Object> reportData) {
    }

    default CheckItemInfo queryCheckItemInfo(String taskId, String rowId) {
        return null;
    }
}
