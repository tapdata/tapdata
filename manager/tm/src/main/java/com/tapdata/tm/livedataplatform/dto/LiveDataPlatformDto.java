package com.tapdata.tm.livedataplatform.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class LiveDataPlatformDto extends BaseDto {
    private String mode;

    private String fdmStorageCluster;

    private String fdmStorageConnectionId;


    private String mdmStorageCluster;

    private String mdmStorageConnectionId;

    private boolean isInit;

}
