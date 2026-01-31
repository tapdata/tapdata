package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import com.tapdata.tm.worker.entity.UsageBase;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private DBCost dBCost;

    public void add(ServerChart.Item item) {
        getRequest().getTs().add(item.getTs());
        getDelay().getTs().add(item.getTs());
        getDBCost().getTs().add(item.getTs());
        if (!item.isTag()) {
            getRequest().add(item);
            getDelay().add(item);
            getDBCost().add(item);
        } else {
            getRequest().addEmpty();
            getDelay().addEmpty();
            getDBCost().addEmpty();
        }
        getDelay().getP95().add(item.getP95());
        getDelay().getP99().add(item.getP99());
        getDBCost().getDbCostP95().add(item.getDbCostP95());
        getDBCost().getDbCostP99().add(item.getDbCostP99());
    }

    @Data
    public static class Usage {
        @DecimalFormat
        List<Double> cpuUsage;
        @DecimalFormat
        List<Double> memoryUsage;

        @DecimalFormat
        List<Double> maxCpuUsage;
        @DecimalFormat
        List<Double> minCpuUsage;
        @DecimalFormat
        List<Double> maxMemoryUsage;
        @DecimalFormat
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

        public void add(UsageBase usage) {
            getCpuUsage().add(usage.getCpuUsage());
            if (null != usage.getHeapMemoryMax()) {
                if (usage.getHeapMemoryMax() > 0L) {
                    getMemoryUsage().add(100.0D * usage.getHeapMemoryUsage() / usage.getHeapMemoryMax());
                } else {
                    getMemoryUsage().add(0D);
                }
            } else {
                getMemoryUsage().add(null);
            }
            ts.add(usage.getLastUpdateTime() / 1000L);
            if (usage instanceof ServerUsageMetric metric) {
                initStatisticsData();
                maxCpuUsage.add(metric.getMaxCpuUsage());
                minCpuUsage.add(metric.getMinCpuUsage());
                if (null != metric.getHeapMemoryMax() && metric.getHeapMemoryMax() > 0L) {
                    Double valueOfMaxMemoryUsage = Optional.ofNullable(metric.getMaxHeapMemoryUsage())
                            .map(v -> v * 100.0 / metric.getHeapMemoryMax())
                            .orElse(null);
                    maxMemoryUsage.add(valueOfMaxMemoryUsage);
                    Double valueOfMinMemoryUsage = Optional.ofNullable(metric.getMinHeapMemoryUsage())
                            .map(v -> v * 100.0 / metric.getHeapMemoryMax())
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
        @DecimalFormat
        List<Double> errorRate;
        List<Long> ts;

        public void add(ServerChart.Item item) {
            getRequestCount().add(item.getRequestCount());
            getErrorRate().add(item.getErrorRate());
        }

        public void addEmpty() {
            getRequestCount().add(0L);
            getErrorRate().add(0D);
        }

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
        @DecimalFormat
        List<Double> avg;
        @DecimalFormat(scale = 1, maxScale = 1)
        List<Double> p95;
        @DecimalFormat(scale = 1, maxScale = 1)
        List<Double> p99;
        @DecimalFormat(scale = 1, maxScale = 1)
        List<Double> maxDelay;
        @DecimalFormat(scale = 1, maxScale = 1)
        List<Double> minDelay;
        List<Long> ts;

        public Delay() {
            this.avg = new ArrayList<>();
            this.p95 = new ArrayList<>();
            this.p99 = new ArrayList<>();
            this.maxDelay = new ArrayList<>();
            this.minDelay = new ArrayList<>();
            this.ts = new ArrayList<>();
        }

        public void add(ServerChart.Item item) {
            getAvg().add(item.getAvg());
            getMaxDelay().add(item.getMaxDelay());
            getMinDelay().add(item.getMinDelay());
        }

        public void addEmpty() {
            getAvg().add(0D);
            getMaxDelay().add(null);
            getMinDelay().add(null);
        }

        public static Delay create() {
            return new Delay();
        }
    }

    @Data
    public static class DBCost {
        @DecimalFormat
        List<Double> dbCostAvg;
        @DecimalFormat(scale = 1, maxScale = 1)
        List<Double> dbCostP95;
        @DecimalFormat(scale = 1, maxScale = 1)
        List<Double> dbCostP99;
        @DecimalFormat(scale = 1, maxScale = 1)
        List<Double> dbCostMax;
        @DecimalFormat(scale = 1, maxScale = 1)
        List<Double> dbCostMin;
        List<Long> ts;

        public void add(ServerChart.Item item) {
            getDbCostAvg().add(item.getDbCostAvg());
            getDbCostMin().add(item.getDbCostMin());
            getDbCostMax().add(item.getDbCostMax());
        }

        public void addEmpty() {
            getDbCostAvg().add(0D);
            getDbCostMin().add(null);
            getDbCostMax().add(null);
        }

        public DBCost() {
            this.dbCostAvg = new ArrayList<>();
            this.dbCostP95 = new ArrayList<>();
            this.dbCostP99 = new ArrayList<>();
            this.dbCostMax = new ArrayList<>();
            this.dbCostMin = new ArrayList<>();
            this.ts = new ArrayList<>();
        }

        public static DBCost create() {
            return new DBCost();
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class Item extends ValueBase.Item {
        Long requestCount;
        Double errorRate;
        Double avg;
        Double p95;
        Double p99;
        Double maxDelay;
        Double minDelay;

        double dbCostAvg;
        Double dbCostMax;
        Double dbCostMin;
        Double dbCostP95;
        Double dbCostP99;

        List<Map<String, Number>> delay = new ArrayList<>();
        List<Map<String, Number>> dbCost = new ArrayList<>();
        Long errorCount;
        boolean tag;


        List<List<Map<String, Number>>> delays;
        List<List<Map<String, Number>>> dbCosts;

        public static Item create(long ts) {
            Item item = new Item();
            item.setTs(ts);
            item.setRequestCount(0L);
            item.setErrorCount(0L);
            item.setTag(true);
            return item;
        }
    }
}
