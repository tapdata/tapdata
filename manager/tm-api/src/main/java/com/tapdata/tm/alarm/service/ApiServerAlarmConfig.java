package com.tapdata.tm.alarm.service;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;

public interface ApiServerAlarmConfig {
    void updateConfig();

    void clean();

    AlarmRuleDto config(String apiId, AlarmKeyEnum alarmKeyEnum);
}
