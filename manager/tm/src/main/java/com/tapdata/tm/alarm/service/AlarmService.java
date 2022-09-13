package com.tapdata.tm.alarm.service;

import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.dto.AlarmListInfoVo;
import com.tapdata.tm.alarm.dto.TaskAlarmInfoVo;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;

import java.util.List;

public interface AlarmService {
    void save(AlarmInfo info);

    boolean checkOpen(String taskId, String nodeId, AlarmKeyEnum key, NotifyEnum notityType);

    List<AlarmRuleDto> findAllRule(String taskId);

    void notifyAlarm();

    void close(String[] ids, UserDetail userDetail);

    Page<AlarmListInfoVo> list(String status,
                               Long start,
                               Long end,
                               String keyword,
                               Integer page,
                               Integer size,
                               UserDetail userDetail);

    TaskAlarmInfoVo listByTask(AlarmStatusEnum status, Level level, String taskId, String nodeId, UserDetail loginUser);
}
