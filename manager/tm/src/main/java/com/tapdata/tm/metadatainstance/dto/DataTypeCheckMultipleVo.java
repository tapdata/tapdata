package com.tapdata.tm.metadatainstance.dto;

import lombok.Data;

@Data
public class DataTypeCheckMultipleVo {
    private boolean result;
    private String originType;
    private Double currentMultiple;
}
