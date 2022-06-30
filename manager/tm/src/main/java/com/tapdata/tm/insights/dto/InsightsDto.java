package com.tapdata.tm.insights.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper=false)
public class InsightsDto extends BaseDto {

    private  String statsCategory;

    private String statsName;

    private Date aggregateTime;

    private Map<String,Object> data;

    private String statsGranularity;

    private Map<String,Object> statsKeys;


    private String statsTime;
}
