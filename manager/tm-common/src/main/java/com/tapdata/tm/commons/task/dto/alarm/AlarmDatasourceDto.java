package com.tapdata.tm.commons.task.dto.alarm;

import com.tapdata.tm.commons.alarm.AlarmComponentEnum;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
@Builder
public class AlarmDatasourceDto {

    private Level level;
    private AlarmComponentEnum component;
    private AlarmStatusEnum status;
    private AlarmTypeEnum type;
    private String agentId;
    private String connectionId;
    private String name;
    private String summary;
    private AlarmKeyEnum metric;
    private Date lastNotifyTime;
    private String userId;

    private Map<String, Object> param;
}
