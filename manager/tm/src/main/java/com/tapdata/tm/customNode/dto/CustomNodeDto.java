package com.tapdata.tm.customNode.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;


/**
 * Logs
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CustomNodeDto extends BaseDto {
    private String name;
    private String desc;
    private String icon;
    private Map<String,Object> formSchema;
    private String template;
}
