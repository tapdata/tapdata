package io.tapdata.supervisor.report;

import java.util.List;

public class TaskThreadInfo {
    private String name;
    private Long id;
    private List<String> methodStacks;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<String> getMethodStacks() {
        return methodStacks;
    }

    public void setMethodStacks(List<String> methodStacks) {
        this.methodStacks = methodStacks;
    }
}
