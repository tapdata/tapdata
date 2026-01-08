package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/30 18:25 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ServerChart extends ValueBase {
    private Usage usage;
    private Request request;
    private Delay delay;

    @Data
    public static class Usage {
        List<Double> cpuUsage;
        List<Double> memoryUsage;

        List<Double> maxCpuUsage;
        List<Double> minCpuUsage;
        List<Double> maxMemoryUsage;
        List<Double> minMemoryUsage;
        List<Long> ts;

        public Usage() {
            this.cpuUsage = new ArrayList<>();
            this.memoryUsage = new ArrayList<>();
            this.ts = new ArrayList<>();
        }

        public static Usage create() {
            return new Usage();
        }

        public void addEmpty(Long timestamp, boolean statistics) {
            ts.add(timestamp);
            getCpuUsage().add(null);
            getMemoryUsage().add(null);
            if (statistics) {
                initStatisticsData();
                maxCpuUsage.add(null);
                minCpuUsage.add(null);
                maxMemoryUsage.add(null);
                minMemoryUsage.add(null);
            }
        }

        void initStatisticsData() {
            if (maxCpuUsage == null) maxCpuUsage = new ArrayList<>();
            if (minCpuUsage == null) minCpuUsage = new ArrayList<>();
            if (maxMemoryUsage == null) maxMemoryUsage = new ArrayList<>();
            if (minMemoryUsage == null) minMemoryUsage = new ArrayList<>();
        }

        public void add(ServerUsage usage) {
            getCpuUsage().add(BigDecimal.valueOf(usage.getCpuUsage()).setScale(2, RoundingMode.HALF_DOWN).doubleValue());
            if (null != usage.getHeapMemoryMax()) {
                if (usage.getHeapMemoryMax() > 0L) {
                    getMemoryUsage().add(BigDecimal.valueOf((100.0D * usage.getHeapMemoryUsage() / usage.getHeapMemoryMax())).setScale(2, RoundingMode.HALF_DOWN).doubleValue());
                } else {
                    getMemoryUsage().add(0D);
                }
            } else {
                getMemoryUsage().add(null);
            }
            ts.add(usage.getLastUpdateTime() / 1000L);
            if (usage instanceof ServerUsageMetric metric) {
                initStatisticsData();
                Double valueOfMaxCpuUsage = Optional.ofNullable(metric.getMaxCpuUsage())
                        .map(BigDecimal::valueOf)
                        .map(v -> v.setScale(2, RoundingMode.HALF_DOWN).doubleValue())
                        .orElse(null);
                maxCpuUsage.add(valueOfMaxCpuUsage);
                Double valueOfMinCpuUsage = Optional.ofNullable(metric.getMinCpuUsage())
                        .map(BigDecimal::valueOf)
                        .map(v -> v.setScale(2, RoundingMode.HALF_DOWN).doubleValue())
                        .orElse(null);
                minCpuUsage.add(valueOfMinCpuUsage);
                if (null != metric.getHeapMemoryMax() && metric.getHeapMemoryMax() > 0L) {
                    Double valueOfMaxMemoryUsage = Optional.ofNullable(metric.getMaxHeapMemoryUsage())
                            .map(v -> v * 100.0 / metric.getHeapMemoryMax())
                            .map(BigDecimal::valueOf)
                            .map(v -> v.setScale(2, RoundingMode.HALF_DOWN).doubleValue())
                            .orElse(null);
                    maxMemoryUsage.add(valueOfMaxMemoryUsage);
                    Double valueOfMinMemoryUsage = Optional.ofNullable(metric.getMinHeapMemoryUsage())
                            .map(v -> v * 100.0 / metric.getHeapMemoryMax())
                            .map(BigDecimal::valueOf)
                            .map(v -> v.setScale(2, RoundingMode.HALF_DOWN).doubleValue())
                            .orElse(null);
                    minMemoryUsage.add(valueOfMinMemoryUsage);
                } else {
                    maxMemoryUsage.add(null);
                    minMemoryUsage.add(null);
                }
            }
        }
    }


    @Data
    public static class Request {
        List<Long> requestCount;
        List<Double> errorRate;
        List<Long> ts;

        public Request() {
            this.requestCount = new ArrayList<>();
            this.errorRate = new ArrayList<>();
            this.ts = new ArrayList<>();
        }

        public static Request create() {
            return new Request();
        }
    }

    @Data
    public static class Delay {
        List<Double> avg;
        List<Long> p95;
        List<Long> p99;
        List<Long> maxDelay;
        List<Long> minDelay;
        List<Long> ts;

        public Delay() {
            this.avg = new ArrayList<>();
            this.p95 = new ArrayList<>();
            this.p99 = new ArrayList<>();
            this.maxDelay = new ArrayList<>();
            this.minDelay = new ArrayList<>();
            this.ts = new ArrayList<>();
        }

        public static Delay create() {
            return new Delay();
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class Item extends ValueBase.Item {
        Long requestCount;
        Double errorRate;
        Double avg;
        Long p95;
        Long p99;
        Long maxDelay;
        Long minDelay;

        public static Item create(long ts) {
            Item item = new Item();
            item.setTs(ts);
            return item;
        }
    }
}
