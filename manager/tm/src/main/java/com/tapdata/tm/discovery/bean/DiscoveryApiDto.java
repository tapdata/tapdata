package com.tapdata.tm.discovery.bean;

import lombok.Data;

import java.util.Date;


/**
 * 服务对象概述
 */
@Data
public class DiscoveryApiDto extends DataDiscoveryDto {
    /** 创建时间 */
    private Date createAt;
    /** 更新时间 */
    private Date lastUpdAt;
    /** 数据项 */
    private Integer inputParamNum;
    private Integer outputParamNum;
    private String description;
}
