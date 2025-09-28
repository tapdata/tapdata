package com.tapdata.tm.worker.dto;

import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/4 15:08 Create
 * @description
 */
@Data
public class ApiServerInfo {
    String processId;

    Integer pid;

    Integer workerPid;

    String name;

    List<ApiServerWorkerInfo> workers;

    Long workerProcessStartTime;

    Long workerProcessEndTime;

    String status;

    Object exitCode;

    MetricInfo metricValues;

    Long pingTime;
}
