package com.tapdata.tm.worker.vo;

import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.MetricInfo;
import lombok.Data;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/2 09:56 Create
 * @description
 */
@Data
public class WorkerOrServerStatus {
    /**
     * API server' process id
     * com.tapdata.tm.apiServer.entity.ApiServerEntity.#processId
     * */
    String processId;

    /**
     * API server' status
     * */
    String status;

    Integer pid;

    /**
     * update timestamp
     * */
    Long time;

    /**
     * worker status Map<work id, work status>
     * */
    Map<String, String> workerStatus;

    MetricInfo processCpuMemStatus;

    Map<String, MetricInfo> cpuMemStatus;

    Map<String, ApiServerWorkerInfo> workerBaseInfo;
}