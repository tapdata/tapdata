package io.tapdata.coding.entity.param;

import java.util.List;

public class IterationParam extends Param {
    private String projectName;
    private List<String> status;
    private List<Integer> assignee;
    private Long startDate;
    private Long endDate;
    private String keywords;

    public static IterationParam create() {
        return new IterationParam();
    }

    public IterationParam projectName(String projectName) {
        this.projectName = projectName;
        return this;
    }

    public IterationParam startDate(Long startDate) {
        this.startDate = startDate;
        return this;
    }

    public IterationParam endDate(Long endDate) {
        this.endDate = endDate;
        return this;
    }

    public IterationParam keywords(String keywords) {
        this.keywords = keywords;
        return this;
    }

    public IterationParam status(List<String> status) {
        this.status = status;
        return this;
    }

    public IterationParam assignee(List<Integer> assignee) {
        this.assignee = assignee;
        return this;
    }

    public String projectName() {
        return this.projectName;
    }

    public Long startDate() {
        return this.startDate;
    }

    public Long endDate() {
        return this.endDate;
    }

    public String keywords() {
        return this.keywords;
    }

    public List<String> status() {
        return this.status;
    }

    public List<Integer> assignee() {
        return this.assignee;
    }
}
