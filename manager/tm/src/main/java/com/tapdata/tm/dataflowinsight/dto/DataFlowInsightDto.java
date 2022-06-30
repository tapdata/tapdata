package com.tapdata.tm.dataflowinsight.dto;

import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;


/**
 * DataFlowInsight
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DataFlowInsightDto extends BaseDto {

    private String statsType;

    private String dataFlowId;

    private String statsTime;

    private String granularity;

    private Map<String, Object> statsData;

}
