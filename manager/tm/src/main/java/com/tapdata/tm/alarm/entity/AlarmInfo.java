package com.tapdata.tm.alarm.entity;

import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.message.constant.Level;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.Map;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "AlarmInfo")
public class AlarmInfo extends BaseEntity {
    @Schema(description = "标记当前告警的状态")
    private AlarmStatusEnum status;
    @Schema(description = "告警级别")
    private Level level;
    @Schema(description = "引擎告警组件固定为引擎")
    private AlarmComponentEnum component;
    @Schema(description = "告警类型 同步任务告警、共享缓存告警、共享挖掘告警、数据校验告警、精准延迟告警")
    private AlarmTypeEnum type;
    @Schema(description = "所属引擎")
    private String agentId;
    private String taskId;
    @Schema(description = "具体的任务名")
    private String name;
    private String nodeId;
    @Schema(description = "产生告警的节点名，无节点时为空;当为任务告警时，节点直接放任务名")
    private String node;
    @Schema(description = "AlarmKeyEnum key 值")
    private AlarmKeyEnum metric;
    @Schema(description = "触发告警的指标值")
    private Integer currentValue;
    @Schema(description = "触发告警的指标阈值")
    private Integer threshold;
    @Schema(description = "告警首次发生时间")
    private Date firstOccurrenceTime;
    @Schema(description = "告警最近发生时间")
    private Date lastOccurrenceTime;
    @Schema(description = "下一次告警通知时间")
    private Date lastNotifyTime;
    @Schema(description = "告警发生次数")
    private Integer tally;
    @Schema(description = "告警内容")
    private String summary;
    @Schema(description = "告警恢复时间")
    private Date recoveryTime;
    @Schema(description = "告警关闭时间")
    private Date closeTime;
    @Schema(description = "告警被谁关闭")
    private String closeBy;

    private Map<String, Object> param;

    public Integer getTally() {
        return tally == null ? 0 : tally;
    }
}
