package com.tapdata.tm.cluster.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * 裸日志服务状态Dto
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class RawServerStateDto extends BaseDto {
    private RawServerInfo reportedData;
    private String dataSource;
    private String serviceId;
    private Date timestamp;

    private String serviceIP;
    private Integer servicePort;

    private Boolean isAlive;
}
