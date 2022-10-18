package com.tapdata.tm.discovery.bean;

import lombok.Data;

@Data
public class TaskConnectionsDto {
    /** 名称 */
    private String name;
    /** 类型 */
    private String type;
    private String connectionName;
    private String connectionInfo;
    private String inputNodeName;
    private String outputNodeName;
    /** 业务描述 */
    private String serviceDesc;
}
