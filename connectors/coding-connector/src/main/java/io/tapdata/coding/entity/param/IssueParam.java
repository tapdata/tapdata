package io.tapdata.coding.entity.param;

import io.tapdata.coding.enums.IssueType;

import java.util.List;
import java.util.Map;

public class IssueParam extends Param {
    private IssueType issueType;
    private List<Map<String, Object>> conditions;
    private String sortKey;
    private String sortValue;
    private Integer issueCode;

    public static IssueParam create() {
        return new IssueParam();
    }

    public IssueParam issueType(IssueType issueType) {
        this.issueType = issueType;
        return this;
    }

    public IssueParam conditions(List<Map<String, Object>> conditions) {
        this.conditions = conditions;
        return this;
    }

    public IssueParam sortKey(String sortKey) {
        this.sortKey = sortKey;
        return this;
    }

    public IssueParam sortValue(String sortValue) {
        this.sortValue = sortValue;
        return this;
    }

    public IssueParam issueCode(Integer issueCode) {
        this.issueCode = issueCode;
        return this;
    }

    public Integer issueCode() {
        return this.issueCode;
    }

    public IssueType issueType() {
        return this.issueType;
    }

    public List<Map<String, Object>> conditions() {
        return this.conditions;
    }

    public String sortKey() {
        return this.sortKey;
    }

    public String sortValue() {
        return this.sortValue;
    }
}
