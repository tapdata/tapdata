package io.tapdata.supervisor.report;

import java.util.List;

public class AliveTask {
    private String name;
    private List<TaskConnector> connectors;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TaskConnector> getConnectors() {
        return connectors;
    }

    public void setConnectors(List<TaskConnector> connectors) {
        this.connectors = connectors;
    }
}
