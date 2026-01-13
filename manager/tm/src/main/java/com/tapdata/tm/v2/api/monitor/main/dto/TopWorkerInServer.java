package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
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
    @DecimalFormat
    Double cpuUsageMax;
    @DecimalFormat
    Double cpuUsageMin;

    List<TopWorkerInServerItem> workerList;

    @Data
    public static class TopWorkerInServerItem {
        String workerId;
        String workerName;
        Long memoryMax;
        Long requestCount = 0L;
        @DecimalFormat
        Double errorRate = 0D;

        ServerChart.Usage usage;
    }
}
