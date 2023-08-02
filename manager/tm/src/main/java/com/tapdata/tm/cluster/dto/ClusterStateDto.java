package com.tapdata.tm.cluster.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;

import java.util.Date;
import java.util.List;


/**
 * 数据源模型
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ClusterStateDto extends BaseDto {

    public static final String ENGINE_STARTING = "starting";
    public static final String ENGINE_RUNNING = "running";
    public static final String ENGINE_RESTARTING = "restarting";
    public static final String ENGINE_STOPPING = "stopping";
    public static final String ENGINE_STOPPED = "stopped";


    private SystemInfo systemInfo;

    private Integer reportInterval;
    private Component engine;

    private Component management;

    private Component apiServer;
    private List<CustomMonitorInfo> customMonitorStatus;
    private String uuid;
    private String status;
    private Date insertTime;
    private Date ttl;

    private String agentName;

    private String custIp;

    private List<CustomMonitorInfo> customMonitor;

}
