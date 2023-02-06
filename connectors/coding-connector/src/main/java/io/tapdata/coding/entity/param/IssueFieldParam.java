package io.tapdata.coding.entity.param;

import java.util.List;

public class IssueFieldParam extends Param {
    public static IssueFieldParam create() {
        return new IssueFieldParam();
    }

    private String issueType;
    private List<String> issueTypes;

    public IssueFieldParam issueType(String issueType) {
        this.issueType = issueType;
        return this;
    }

    public String issueType() {
        return this.issueType;
    }

    public IssueFieldParam issueTypes(List<String> issueTypes) {
        this.issueTypes = issueTypes;
        return this;
    }

    public List<String> issueTypes() {
        return this.issueTypes;
    }

}
