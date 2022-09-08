package com.tapdata.tm.alarm.service;

import com.tapdata.tm.alarm.entity.AlarmInfo;

public interface AlarmService {
    void save(AlarmInfo info);

    boolean checkOpen(String taskId, String key, String notityType);
}
