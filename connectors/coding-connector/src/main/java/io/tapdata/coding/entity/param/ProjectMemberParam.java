package io.tapdata.coding.entity.param;

public class ProjectMemberParam extends Param {
    private Integer projectId;
    private Integer roleId;

    public static ProjectMemberParam create() {
        return new ProjectMemberParam();
    }

    public ProjectMemberParam projectId(Integer projectId) {
        this.projectId = projectId;
        return this;
    }

    public ProjectMemberParam roleId(Integer roleId) {
        this.roleId = roleId;
        return this;
    }

    public Integer projectId() {
        return this.projectId;
    }

    public Integer roleId() {
        return this.roleId;
    }
}
