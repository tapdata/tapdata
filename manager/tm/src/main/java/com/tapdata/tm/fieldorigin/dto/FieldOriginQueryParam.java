package com.tapdata.tm.fieldorigin.dto;

import lombok.Data;

import java.util.List;

@Data
public class FieldOriginQueryParam {
    private String taskId;
    /** Optional, active version when empty */
    private Long version;
    /** Optional target table filters, a task may write many target tables */
    private String targetConnectionId;
    private String targetTable;
    /** Changed field paths, a[0].b / a.0.b are normalized to a[].b; a container path also returns its children */
    private List<String> fieldPaths;
}
