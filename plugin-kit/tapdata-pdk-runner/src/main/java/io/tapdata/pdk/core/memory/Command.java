package io.tapdata.pdk.core.memory;

import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

public class Command {

    private List<Execution> executionList;

    public List<Execution> getExecutionList() {
        return executionList;
    }

    public void setExecutionList(List<Execution> executionList) {
        this.executionList = executionList;
    }

}
