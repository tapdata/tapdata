package com.tapdata.taskinspect;

import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.cons.JobType;
import com.tapdata.tm.taskinspect.dto.TaskInspectHistoriesDto;
import com.tapdata.tm.taskinspect.vo.JobReportVo;
import com.tapdata.tm.taskinspect.vo.ResultsRecoverVo;
import com.tapdata.tm.taskinspect.vo.ResultsReportVo;

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

    default TaskInspectHistoriesDto jobStart(String taskId, JobType type, Serializable config, Map<String, Serializable> ext) {
        return null;
    }

    default boolean postJobStatus(String jobId, JobReportVo vo) {
        return false;
    }

    default void postResults(String jobId, ResultsReportVo... vos) {
    }

    default void postRecover(String jobId, ResultsRecoverVo vo) {
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

}
