package com.tapdata.tm.lineagegraph.dto;

import com.tapdata.tm.lineagegraph.bean.LineageValue;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;

import java.util.Date;


/**
 * LineageGraph
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class LineageGraphDto extends BaseDto {
    private String v;
    private String w;
    private LineageValue value;


    private String type;
    private String status;
    private Integer currProgress;
    private String ipv4;
    private Integer pid;
    private Double ping_time;
    private Date start_data;
    private Integer allProgress;
    private String version;
    private Date finish_date;

}
