package com.tapdata.tm.alarm.service;

import com.tapdata.tm.alarm.entity.AlarmInfo;

public interface ApiServerAlarmService {

    default void scanMetricData() {

    }

    default void saveAlarmInfo(AlarmInfo alarmInfo) {
    }
}
