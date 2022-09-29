package com.tapdata.tm.discovery.bean;

import lombok.Data;

import java.util.Date;


/**
 * 存储对象概述
 */
@Data
public class DiscoveryStorageDto extends DataDiscoveryDto {
    /** 创建时间 */
    private Date createAt;
    /** 更新时间 */
    private Date lastUpdAt;
    /** 数据项 */
    private Integer fieldNum;
    /** 数据量 */
    private Long rowNum;
    /** 连接名称 */
    private String connectionName;
    /** 连接类型 */
    private String connectionType;
    /** 连接描述 */
    private String connectionDesc;
    private String version;
}
