package com.tapdata.tm.alarm.service;

import com.tapdata.tm.alarm.dto.AlarmChannelDto;
import com.tapdata.tm.alarm.dto.AlarmListInfoVo;
import com.tapdata.tm.alarm.dto.AlarmListReqDto;
import com.tapdata.tm.alarm.dto.TaskAlarmInfoVo;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.commons.task.dto.alarm.AlarmVO;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.dto.MessageDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Locale;
import java.util.Map;

class AlarmServiceTest {
    AlarmService service = new AlarmService() {
        @Override
        public void save(AlarmInfo info) {

        }

        @Override
        public Map<String, List<AlarmRuleDto>> getAlarmRuleDtos(TaskDto taskDto) {
            return Map.of();
        }

        @Override
        public void close(String[] ids, UserDetail userDetail) {

        }

        @Override
        public Page<AlarmListInfoVo> list(String status, Long start, Long end, String keyword, Integer page, Integer size, UserDetail userDetail, Locale locale) {
            return null;
        }

        @Override
        public TaskAlarmInfoVo listByTask(AlarmListReqDto dto) {
            return null;
        }

        @Override
        public List<AlarmInfo> find(String taskId, String nodeId, AlarmKeyEnum key) {
            return List.of();
        }

        @Override
        public void closeWhenTaskRunning(String taskId) {

        }

        @Override
        public void delAlarm(String taskId) {

        }

        @Override
        public List<AlarmInfo> query(Query query) {
            return List.of();
        }

        @Override
        public MessageDto add(MessageDto messageDto, UserDetail userDetail) {
            return null;
        }

        @Override
        public List<AlarmChannelDto> getAvailableChannels() {
            return List.of();
        }

        @Override
        public boolean checkOpen(TaskDto taskDto, String nodeId, AlarmKeyEnum key, NotifyEnum type, List<AlarmSettingDto> settingDtos) {
            return false;
        }

        @Override
        public boolean checkOpen(TaskDto taskDto, String nodeId, AlarmKeyEnum key, NotifyEnum type, UserDetail userDetail) {
            return false;
        }

        @Override
        public boolean checkOpen(List<AlarmSettingVO> alarmSettingVOS, AlarmKeyEnum key, NotifyEnum type, UserDetail userDetail) {
            return false;
        }

        @Override
        public void closeWhenInspectTaskRunning(String id) {

        }

        @Override
        public void updateTaskAlarm(AlarmVO alarm) {

        }

        @Override
        public void taskRetryAlarm(String taskId, Map<String, Object> params) {

        }
    };

    @Test
    void testNotifyAlarm() {
        Assertions.assertDoesNotThrow(service::notifyAlarm);
    }
}