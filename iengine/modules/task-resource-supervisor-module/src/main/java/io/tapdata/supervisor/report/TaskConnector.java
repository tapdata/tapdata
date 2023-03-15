package io.tapdata.supervisor.report;

import java.util.List;
import java.util.Map;

public class TaskConnector {
    private String associateId;
    private String id;
    private int threadCount;
    private List<TaskThreadInfo> threads;
    private List<Map<String, Object>> resources;

    public String getAssociateId() {
        return associateId;
    }

    public void setAssociateId(String associateId) {
        this.associateId = associateId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public List<TaskThreadInfo> getThreads() {
        return threads;
    }

    public void setThreads(List<TaskThreadInfo> threads) {
        this.threads = threads;
    }

    public List<Map<String, Object>> getResources() {
        return resources;
    }

    public void setResources(List<Map<String, Object>> resources) {
        this.resources = resources;
    }
}
