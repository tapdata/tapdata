package com.tapdata.tm.group.vo;

import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class ResourceDiff {
    private List<ResourceDiffItem> add    = new ArrayList<>();
    private List<ResourceDiffItem> update = new ArrayList<>();
    private List<ResourceDiffItem> delete = new ArrayList<>();
}