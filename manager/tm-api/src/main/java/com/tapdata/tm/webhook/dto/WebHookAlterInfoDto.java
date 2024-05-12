package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.message.constant.Level;
import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
public class WebHookAlterInfoDto extends BaseDto {
    private AlarmStatusEnum status;
    private Level level;
    private AlarmComponentEnum component;
    private AlarmTypeEnum type;
    private String agentId;
    private String taskId;
    private String name;
    private String nodeId;
    private String node;
    private AlarmKeyEnum metric;
    private Integer currentValue;
    private Integer threshold;
    private Date firstOccurrenceTime;
    private Date lastOccurrenceTime;
    private Date lastNotifyTime;
    private Integer tally;
    private String summary;
    private Date recoveryTime;
    private Date closeTime;
    private String closeBy;
    private String inspectId;

    private transient Map<String, Object> param;

}
