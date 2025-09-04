package com.tapdata.tm.worker.dto;

import lombok.Data;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/4 10:30 Create
 * @description
 */
@Data
public class ApiServerWorkerInfo {

    String oid;

    String name;

    Integer id;

    Integer pid;

    String workerStatus;

    Long workerStartTime;

    MetricInfo metricValues;

    int sort;
}
