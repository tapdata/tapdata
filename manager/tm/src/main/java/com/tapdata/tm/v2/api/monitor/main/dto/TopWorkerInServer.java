package com.tapdata.tm.v2.api.monitor.main.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:00 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TopWorkerInServer extends ValueBase {
    Double cpuUsageMax;
    Double cpuUsageMin;

    List<TopWorkerInServerItem> workerList;

    @Data
    public static class TopWorkerInServerItem {
        String workerId;
        String workerName;
        Long memoryMax;
        Long requestCount;
        Double errorRate;

        ServerChart.Usage usage;
    }
}
