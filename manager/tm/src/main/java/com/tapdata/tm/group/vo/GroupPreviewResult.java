package com.tapdata.tm.group.vo;

import lombok.Data;

@Data
public class GroupPreviewResult {
    private ResourceDiff connections = new ResourceDiff();
    private ResourceDiff tasks       = new ResourceDiff();
    private ResourceDiff apis        = new ResourceDiff();
}