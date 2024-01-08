package com.tapdata.tm.commons.metrics;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ConnectorRecordFlag {
    private String processId;
    private Long version;
    private String pdkHash;
    private Boolean flag;
    private Boolean isOver;
}
