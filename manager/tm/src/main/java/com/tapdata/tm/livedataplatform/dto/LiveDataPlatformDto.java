package com.tapdata.tm.livedataplatform.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


/**
 * LineageGraph
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class LiveDataPlatformDto extends BaseDto {
    private String mode;

    private String fdmStorageCluster;

    private String fdmStorageAddress;


    private String mdmStorageCluster;


    private String mdmStorageAddress;

}
