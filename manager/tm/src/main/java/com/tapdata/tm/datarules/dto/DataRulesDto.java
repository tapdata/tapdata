package com.tapdata.tm.datarules.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;

import java.util.List;


/**
 * DataRules
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DataRulesDto extends BaseDto {
    private List<String> rules;
    private String name;

}
