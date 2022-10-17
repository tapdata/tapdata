package com.tapdata.tm.schedule.service;

import com.tapdata.tm.schedule.entity.ScheduleJobInfo;

import java.util.List;

public interface ScheduleJobService {
    void save(List<ScheduleJobInfo> jobs);

    List<ScheduleJobInfo> listByCode(String code);
}
