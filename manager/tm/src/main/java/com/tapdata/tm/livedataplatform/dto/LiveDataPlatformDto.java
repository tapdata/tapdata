package com.tapdata.tm.livedataplatform.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class LiveDataPlatformDto extends BaseDto {
    private String mode; // 数据集成 integration, 数据服务 service

    private String fdmStorageCluster; // 自建: self、全托管: full-management

    private String fdmStorageConnectionId;


    private String mdmStorageCluster;

    private String mdmStorageConnectionId;

    private boolean isInit;

}
