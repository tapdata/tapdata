package com.tapdata.tm.inspect.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;

import java.util.Date;
import java.util.Map;


/**
 * 数据校验详情
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class InspectDetailsDto extends BaseDto {
    /**  */
    private String inspect_id;
    /**  */
    private String taskId;
    /**  */
    private String type;

    private Map<String, Object> source;
    private Map<String, Object> target;


    /**  */
    private String inspectResultId;
    /**  */
    private String message;
    /**  */
    private Date ttlTime;
}
