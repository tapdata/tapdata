package com.tapdata.tm.alarm.service;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;

public interface ApiServerAlarmConfig {
    String SYSTEM = "system";

    default void updateConfig() {}

    default void clean() {}

    default void remove(String apiId, AlarmKeyEnum alarmKeyEnum) {}

    default void update(String apiId, AlarmKeyEnum alarmKeyEnum) {}

    AlarmRuleDto config(String apiId, AlarmKeyEnum alarmKeyEnum);
}
