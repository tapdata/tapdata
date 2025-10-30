package com.tapdata.tm.worker.dto;

import lombok.Data;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/17 14:44 Create
 * @description
 */
@Data
public class ApiServerWorkerInfo {
    Long activeTime;

    Long pingTime;

    Integer id;

    Integer pid;

    String oid;

    String name;

    String workerStatus;

    Long workerStartTime;

    MetricInfo metricValues;

    int sort;

    /**
     * 2: be used
     * 0: not used
     * */
    int tag;
}
