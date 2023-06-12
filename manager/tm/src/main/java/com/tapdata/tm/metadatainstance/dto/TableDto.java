package com.tapdata.tm.metadatainstance.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TableDto {
    private String tableName;

    private String tableComment;
}
