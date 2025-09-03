package com.tapdata.tm.apiCalls.vo;

import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 12:25 Create
 * @description
 */
@Data
public class ApiCountMetricVo {
    private ProcessMetric processMetric;

    private List<WorkerMetrics> workerMetrics;

    @Data
    public static class ProcessMetric {
        private String processId;
        private String serverName;
        ApiItem processMetric;
    }

    @Data
    public static class WorkerMetrics {
        private String workOid;
        private String workerName;
        private ApiItem workerMetric;
    }

    @Data
    public static class ApiItem {
        String apiId;
        String apiName;
        Long count;
        Long errorCount;
    }
}
