package com.tapdata.tm.discovery.bean;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.modules.entity.Path;
import lombok.Data;

import java.util.List;

/**
 * 服务对象概览
 */
@Data
public class DiscoveryApiOverviewDto extends DiscoveryApiDto {
    private List<Path> paths;
    private List<Field> fields;
}
