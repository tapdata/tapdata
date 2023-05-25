package com.tapdata.tm.lineage.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


/**
 * Lineage
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class LineageDto extends BaseDto {
    private String name;

}