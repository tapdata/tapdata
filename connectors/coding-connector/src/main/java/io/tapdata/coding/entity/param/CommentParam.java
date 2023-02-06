package io.tapdata.coding.entity.param;

import java.util.List;

/**
 * 事项评论查询参数
 */
public class CommentParam extends Param {
    private String projectName;
    private Integer issueCode;
    private List<Integer> issueCodes;

    public static CommentParam create() {
        return new CommentParam();
    }

    public CommentParam projectName(String projectName) {
        this.projectName = projectName;
        return this;
    }

    public CommentParam issueCode(Integer issueCode) {
        this.issueCode = issueCode;
        return this;
    }

    public CommentParam issueCodes(List<Integer> issueCodes) {
        this.issueCodes = issueCodes;
        return this;
    }

    public String projectName() {
        return this.projectName;
    }

    public Integer issueCode() {
        return this.issueCode;
    }

    public List<Integer> issueCodes() {
        return this.issueCodes;
    }

}
