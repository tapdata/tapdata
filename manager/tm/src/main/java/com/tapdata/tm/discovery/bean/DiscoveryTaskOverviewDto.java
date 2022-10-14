package com.tapdata.tm.discovery.bean;

import lombok.Data;

import java.util.List;

/**
 * 任务对象概览
 */
@Data
public class DiscoveryTaskOverviewDto extends DiscoveryTaskDto {
    private List<TaskConnectionsDto> taskConnections;
}
