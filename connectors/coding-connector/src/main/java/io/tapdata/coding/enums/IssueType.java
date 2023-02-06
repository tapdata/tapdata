package io.tapdata.coding.enums;

public enum IssueType {
    ALL("ALL", "全部事项"),
    DEFECT("DEFECT", "缺陷"),
    REQUIREMENT("REQUIREMENT", "需求"),
    MISSION("MISSION", "任务"),
    EPIC("EPIC", "史诗");

    String name;
    String description;

    IssueType(String name, String description) {
        this.description = description;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static String verifyType(String typeName) {
        if (null == typeName || "".equals(typeName)) return ALL.name;
        IssueType[] values = IssueType.values();
        for (IssueType value : values) {
            if (value.name.equals(typeName)) {
                return value.name;
            }
        }
        return ALL.name;
    }

    public static IssueType issueType(String typeName) {
        if (null == typeName || "".equals(typeName)) return ALL;
        IssueType[] values = IssueType.values();
        for (IssueType value : values) {
            if (value.name.equals(typeName)) {
                return value;
            }
        }
        return ALL;
    }
}
