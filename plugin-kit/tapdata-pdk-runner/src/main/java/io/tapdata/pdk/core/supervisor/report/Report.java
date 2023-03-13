package io.tapdata.pdk.core.supervisor.report;

import java.util.List;

public class Report {
    private int aliveTaskCount;
    private int leakedTaskCount;
    private List<AliveTask> aliveTasks;
    private List<AliveTask> leakedTasks;
    private int aliveConnectorCount;
    private List<AliveTask> aliveConnectors;
    private List<AliveTask> leakedConnectors;

    public int getAliveTaskCount() {
        return aliveTaskCount;
    }

    public void setAliveTaskCount(int aliveTaskCount) {
        this.aliveTaskCount = aliveTaskCount;
    }

    public int getLeakedTaskCount() {
        return leakedTaskCount;
    }

    public void setLeakedTaskCount(int leakedTaskCount) {
        this.leakedTaskCount = leakedTaskCount;
    }

    public List<AliveTask> getAliveTasks() {
        return aliveTasks;
    }

    public void setAliveTasks(List<AliveTask> aliveTasks) {
        this.aliveTasks = aliveTasks;
    }

    public List<AliveTask> getLeakedTasks() {
        return leakedTasks;
    }

    public void setLeakedTasks(List<AliveTask> leakedTasks) {
        this.leakedTasks = leakedTasks;
    }

    public int getAliveConnectorCount() {
        return aliveConnectorCount;
    }

    public void setAliveConnectorCount(int aliveConnectorCount) {
        this.aliveConnectorCount = aliveConnectorCount;
    }

    public List<AliveTask> getAliveConnectors() {
        return aliveConnectors;
    }

    public void setAliveConnectors(List<AliveTask> aliveConnectors) {
        this.aliveConnectors = aliveConnectors;
    }

    public List<AliveTask> getLeakedConnectors() {
        return leakedConnectors;
    }

    public void setLeakedConnectors(List<AliveTask> leakedConnectors) {
        this.leakedConnectors = leakedConnectors;
    }
}
