package com.tapdata.tm.discovery.bean;

import lombok.Data;

import java.util.List;

/**
 * 存储对象概览
 */
@Data
public class DiscoveryStorageOverviewDto extends DiscoveryStorageDto {
    private List<DiscoveryFieldDto> fields;
    private String comment;
    private String description;

}
