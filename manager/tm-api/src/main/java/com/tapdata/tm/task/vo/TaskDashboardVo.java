package com.tapdata.tm.task.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class TaskDashboardVo {
    private QueryInfo query;
    private Summary summary;
    private Trends trends;
    private Tops tops;

    @Data
    public static class QueryInfo {
        private String type;
        private long step;
        private String dashboardType;
        private int top;
        private long startAt;
        private long endAt;
    }

    @Data
    public static class Summary {
        private ActiveTasks activeTasks;
        private TotalThroughput totalThroughput;
        private ConnectedDbs connectedDbs;
        private ApiRequests apiRequests;
    }

    @Data
    public static class ActiveTasks {
        private int total;
        private int running;
        private int error;
        private long maxLag;
        private long minLag;
    }

    @Data
    public static class TotalThroughput {
        private double current;
        private double peak;
        private double dataRate;
        private double changeRate;
    }

    @Data
    public static class ConnectedDbs {
        private int total;
        private List<DbItem> items = new ArrayList<>();
    }

    @Data
    public static class DbItem {
        private String id;
        private String name;
        private long tableCount;
    }

    @Data
    public static class ApiRequests {
        private long total;
        private long failed;
        private double errorRate;
        private double avgTime;
    }

    @Data
    public static class Trends {
        private Trend throughput;
        private Trend apiRequests;
    }

    @Data
    public static class Trend {
        private List<Long> ts = new ArrayList<>();
        private List<Double> values = new ArrayList<>();
    }

    @Data
    public static class Tops {
        private List<TaskRankItem> topLaggingTasks = new ArrayList<>();
        private List<TaskRankItem> topThroughputTasks = new ArrayList<>();
    }

    @Data
    public static class TaskRankItem {
        private String taskId;
        private String taskName;
        private long latency;
        private double throughput;
    }
}
