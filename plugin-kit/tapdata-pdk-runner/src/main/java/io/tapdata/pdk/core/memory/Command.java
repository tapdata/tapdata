package io.tapdata.pdk.core.memory;

import java.util.List;

public class Command {

    private List<Execution> executionList;

    public List<Execution> getExecutionList() {
        return executionList;
    }

    public void setExecutionList(List<Execution> executionList) {
        this.executionList = executionList;
    }
}
