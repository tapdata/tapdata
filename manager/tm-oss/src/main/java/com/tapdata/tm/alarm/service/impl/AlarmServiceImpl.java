package com.tapdata.tm.alarm.service.impl;

import com.tapdata.tm.alarm.dto.*;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.commons.task.dto.alarm.AlarmVO;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.dto.MessageDto;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class AlarmServiceImpl implements AlarmService {
    private final static String ERROR = "TapOssNonSupportFunctionException";
    @Override
    public void save(AlarmInfo info) {
        throw new BizException(ERROR);
    }

    @Override
    public Map<String, List<AlarmRuleDto>> getAlarmRuleDtos(TaskDto taskDto) {
        return null;
    }

    @Override
    public void close(String[] ids, UserDetail userDetail) {
        throw new BizException(ERROR);
    }

    @Override
    public Page<AlarmListInfoVo> list(String status, Long start, Long end, String keyword, Integer page, Integer size, UserDetail userDetail, Locale locale) {
        throw new BizException(ERROR);
    }

    @Override
    public TaskAlarmInfoVo listByTask(AlarmListReqDto dto) {
        return TaskAlarmInfoVo.builder()
                .nodeInfos(new ArrayList<>())
                .alarmList(new ArrayList<>())
                .build();
    }

    @Override
    public List<AlarmInfo> find(String taskId, String nodeId, AlarmKeyEnum key) {
        throw new BizException(ERROR);
    }

    @Override
    public void closeWhenTaskRunning(String taskId) {
        throw new BizException(ERROR);
    }

    @Override
    public void delAlarm(String taskId) {
    }

    @Override
    public List<AlarmInfo> query(Query query) {
        return new ArrayList<>();
    }

    @Override
    public MessageDto add(MessageDto messageDto, UserDetail userDetail) {
        throw new BizException(ERROR);
    }

    @Override
    public List<AlarmChannelDto> getAvailableChannels() {
        return new ArrayList<>();
    }

    @Override
    public boolean checkOpen(TaskDto taskDto, String nodeId, AlarmKeyEnum key, NotifyEnum type, List<AlarmSettingDto> settingDtos) {
        throw new BizException(ERROR);
    }

    @Override
    public boolean checkOpen(TaskDto taskDto, String nodeId, AlarmKeyEnum key, NotifyEnum type, UserDetail userDetail) {
        throw new BizException(ERROR);
    }

    @Override
    public boolean checkOpen(List<AlarmSettingVO> alarmSettingVOS, AlarmKeyEnum key, NotifyEnum type, UserDetail userDetail) {
        throw new BizException(ERROR);
    }

    @Override
    public void closeWhenInspectTaskRunning(String id) {
        throw new BizException(ERROR);
    }

    @Override
    public void updateTaskAlarm(AlarmVO alarm) {
        throw new BizException(ERROR);
    }

    @Override
    public void taskRetryAlarm(String taskId, Map<String, Object> params) {
        throw new BizException(ERROR);
    }
}
