package com.tapdata.tm.commons.task.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CheckTaskMemoryParam {
    private long batchSize;
    private int nodeSize;
    private int writeBatchSize;
    private String connectionId;
    private Map<String,Long> tableMap;
}
