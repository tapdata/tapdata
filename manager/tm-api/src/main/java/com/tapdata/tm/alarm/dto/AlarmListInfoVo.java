package com.tapdata.tm.alarm.dto;

import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.message.constant.Level;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@Builder
@Data
public class AlarmListInfoVo {
    private String id;
    @Schema(description = "告警级别")
    private Level level;
    @Schema(description = "标记当前告警的状态")
    private AlarmStatusEnum status;
    @Schema(description = "具体的任务名")
    private String name;
    @Schema(description = "告警内容")
    private String summary;
    @Schema(description = "告警首次发生时间")
    private Date firstOccurrenceTime;
    @Schema(description = "告警最近发生时间")
    private Date lastOccurrenceTime;
    @Schema(description = "下一次告警通知时间")
    private Date lastNotifyTime;
    private String taskId;
    private String nodeId;
    private AlarmKeyEnum metric;
    private String syncType;
}
