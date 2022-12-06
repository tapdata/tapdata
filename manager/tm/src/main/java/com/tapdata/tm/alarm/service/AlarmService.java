package com.tapdata.tm.alarm.service;

import com.tapdata.tm.alarm.dto.AlarmListInfoVo;
import com.tapdata.tm.alarm.dto.AlarmListReqDto;
import com.tapdata.tm.alarm.dto.TaskAlarmInfoVo;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.config.security.UserDetail;

import java.util.List;
import java.util.Map;

public interface AlarmService {
    void save(AlarmInfo info);

    Map<String, List<AlarmRuleDto>> getAlarmRuleDtos(TaskDto taskDto);

    void notifyAlarm();

    void close(String[] ids, UserDetail userDetail);

    Page<AlarmListInfoVo> list(String status,
                               Long start,
                               Long end,
                               String keyword,
                               Integer page,
                               Integer size,
                               UserDetail userDetail);

    TaskAlarmInfoVo listByTask(AlarmListReqDto dto);

    List<AlarmInfo> find(String taskId, String nodeId, AlarmKeyEnum key);

    void closeWhenTaskRunning(String taskId);
    void connectAlarm(String nodeName, String connectId, String response_body, boolean pass);

    void delAlarm(String taskId);
}
