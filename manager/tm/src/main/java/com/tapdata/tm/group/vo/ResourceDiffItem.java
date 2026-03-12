package com.tapdata.tm.group.vo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class ResourceDiffItem {
    private String name;
    /** for tasks: migrate / sync / validate / shareCache; null for others */
    private String type;
    /** 字段级变更列表，add 项为 null */
    private List<FieldChange> changes;

    public ResourceDiffItem(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public ResourceDiffItem(String name, String type, List<FieldChange> changes) {
        this.name = name;
        this.type = type;
        this.changes = changes;
    }
}