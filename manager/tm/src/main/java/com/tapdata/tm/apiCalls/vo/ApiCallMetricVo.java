package com.tapdata.tm.apiCalls.vo;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 12:06 Create
 * @description
 */
@Data
public class ApiCallMetricVo {

    private ProcessMetric processMetric;

    private List<WorkerMetrics> workerMetrics = new ArrayList<>();

    @Data
    public static class ProcessMetric {
        private String processId;
        private String serverName;
        MetricBase processMetric;
    }

    @Data
    public static class WorkerMetrics {
        private String workOid;
        private String workerName;
        private MetricBase workerMetric;
    }


    @Data
    public static class MetricBase {
        List<Long> time;

        public MetricBase() {
            this.time = new ArrayList<>();
        }

        protected void add(Long time) {
            this.time.add(time);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class MetricRPS extends MetricBase {
        List<Double> rps;

        public MetricRPS() {
            super();
            this.rps = new ArrayList<>();
        }

        public void add(Long time, Double rps) {
            super.add(time);
            this.rps.add(rps);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class MetricResponseTime extends MetricBase {
        List<Long> p50;
        List<Long> p95;
        List<Long> p99;

        public MetricResponseTime() {
            super();
            this.p50 = new ArrayList<>();
            this.p95 = new ArrayList<>();
            this.p99 = new ArrayList<>();
        }

        public void add(Long time, Long p50, Long p95, Long p99) {
            super.add(time);
            this.p50.add(p50);
            this.p95.add(p95);
            this.p99.add(p99);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class MetricErrorRate extends MetricBase {
        List<Double> errorRate;

        public MetricErrorRate() {
            super();
            this.errorRate = new ArrayList<>();
        }

        public void add(Long time, Double errorRate) {
            super.add(time);
            this.errorRate.add(errorRate);
        }
    }
}
