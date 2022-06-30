package com.tapdata.tm.dataflowsdebug.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;


/**
 * DataFlowsDebug
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DataFlowsDebugDto extends BaseDto {
    private String name;

}
