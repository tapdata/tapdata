package com.tapdata.tm.inspect.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.inspect.dto.InspectCdcRunProfiles;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;

/**
 * @Author: Zed
 * @Date: 2021/9/14
 * @Description:
 */
@Data
public class Stats {
    private String taskId;
    private Source source;
    private Source target;
    private Date start;
    private Date end;
    private String status;
    private String errorMsg;
    private String result;
    private Double progress;
    private Long cycles;

    @JsonProperty("source_total")
    @Field("source_total")
    private Long sourceTotal;

    @JsonProperty("target_total")
    @Field("target_total")
    private Long targetTotal;

    private Long both;

    @JsonProperty("source_only")
    @Field("source_only")
    private Long sourceOnly;

    @JsonProperty("target_only")
    @Field("target_only")
    private Long targetOnly;

    @JsonProperty("row_passed")
    @Field("row_passed")
    private Long rowPassed;

    @JsonProperty("row_failed")
    @Field("row_failed")
    private Long rowFailed;
    private Long speed;
    private long firstSourceTotal;
    private long firstTargetTotal;
    private InspectCdcRunProfiles cdcRunProfiles;
}
