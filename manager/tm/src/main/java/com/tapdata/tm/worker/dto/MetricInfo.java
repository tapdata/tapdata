package com.tapdata.tm.worker.dto;

import lombok.Data;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/4 15:11 Create
 * @description
 */
@Data
public class MetricInfo {
    /**
     * unit byte
     * */
    Long heapMemoryUsage;

    /**
     * unit %, such as 0.1 means 10%
     * */
    Long cpuUsage;

    /**
     * timestamp, unit ms
     * */
    Long lastUpdateTime;
}
