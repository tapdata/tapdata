package com.tapdata.tm.alarm.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "AlarmInfo")
public class AlarmInfo extends BaseEntity {
    private String status;
    private String level;
    private String component;
    private String type;
    private String agnetId;
    private String name;
    private String node;
    @Schema(description = "AlarmKeyEnum key 值")
    private String metric;
    @Schema(description = "触发告警的指标值")
    private Integer currentValue;
    @Schema(description = "触发告警的指标阈值")
    private Integer threshold;
    @Schema(description = "告警首次发生时间")
    private Date firstOccurrenceTime;
    @Schema(description = "告警最近发生时间")
    private Date lastOccurrenceTime;
    @Schema(description = "告警发生次数")
    private Integer tally;
    @Schema(description = "告警内容")
    private String summary;
    private Date recoveryTime;
    private Date closeTime;
    private String closeBy;
}
