package com.tapdata.tm.lineagegraph.entity;

import com.tapdata.tm.lineagegraph.bean.LineageValue;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;


/**
 * LineageGraph
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("LineageGraph")
public class LineageGraphEntity extends BaseEntity {
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