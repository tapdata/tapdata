package com.tapdata.tm.metadatainstance.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class MigrateResetTableDto {
    private String taskId;
    private String nodeId;
}
