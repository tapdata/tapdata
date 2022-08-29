package io.tapdata.coding.enums;

public enum IssueType {
    ALL("ALL","全部事项"),
    DEFECT("DEFECT","缺陷"),
    REQUIREMENT("REQUIREMENT","需求"),
    MISSION("MISSION","任务"),
    EPIC("EPIC","史诗");

    String name;
    String description;
    IssueType(String name,String description){
        this.description = description;
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
