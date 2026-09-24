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
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.alarm.AlarmImpactView;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverCandidates;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverPreview;
import com.tapdata.tm.commons.task.dto.alarm.AlarmVO;
import com.tapdata.tm.commons.task.dto.alarm.BatchAlarmDetail;
import com.tapdata.tm.commons.task.dto.alarm.BatchAlarmResult;
import com.tapdata.tm.commons.task.dto.alarm.BatchUpdateAlarmParam;
import com.tapdata.tm.commons.task.dto.alarm.TaskAlertRequest;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.dto.MessageDto;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public interface AlarmService {
    void save(AlarmInfo info);

    Map<String, List<AlarmRuleDto>> getAlarmRuleDtos(TaskDto taskDto);

    /**
     * @deprecated
     * @description use com.tapdata.tm.alarm.service.impl.Notifier to implement your notify logic
     *              and AlarmNotifyServiceImpl will auto register it to notify service info
     * */
    @Deprecated(since = "release-v4.9.0", forRemoval = true)
    default void notifyAlarm() {
        //do nothing
    }

    void close(String[] ids, UserDetail userDetail);

    Page<AlarmListInfoVo> list(String status,
                               Long start,
                               Long end,
                               String keyword,
                               Integer page,
                               Integer size,
                               UserDetail userDetail,
                               Locale locale);

    TaskAlarmInfoVo listByTask(AlarmListReqDto dto);

    List<AlarmInfo> find(String taskId, String nodeId, AlarmKeyEnum key);

    void closeWhenTaskRunning(String taskId);


    void delAlarm(String taskId);

    List<AlarmInfo> query(Query query);

    MessageDto add(MessageDto messageDto,UserDetail userDetail);

    List<AlarmChannelDto> getAvailableChannels();

    boolean checkOpen(TaskDto taskDto, String nodeId, AlarmKeyEnum key, NotifyEnum type, List<AlarmSettingDto> settingDtos);

    boolean checkOpen(TaskDto taskDto, String nodeId, AlarmKeyEnum key, NotifyEnum type, UserDetail userDetail);

	boolean checkOpen(List<AlarmSettingVO> alarmSettingVOS, AlarmKeyEnum key, NotifyEnum type, UserDetail userDetail);

	void closeWhenInspectTaskRunning(String id);

    void updateTaskAlarm(AlarmVO alarm);

    default void updateTaskAlarm(AlarmVO alarm, UserDetail userDetail) {
        updateTaskAlarm(alarm);
    }

    void taskRetryAlarm(String taskId,Map<String, Object> params);

    void batchUpdate(BatchUpdateAlarmParam alarm);

    default BatchAlarmDetail applyAuthorizedTaskAlarm(String taskId, BatchUpdateAlarmParam alarm, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    default void runWithReceiverCache(Runnable action) {
        if (action != null) {
            action.run();
        }
    }

    default void fillAlarmReceiverSummary(List<TaskDto> tasks) {
    }

    default AlarmReceiverPreview previewReceivers(String taskId) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    default AlarmReceiverPreview previewReceivers(String taskId, String userId) {
        return previewReceivers(taskId);
    }

    default AlarmReceiverCandidates receiverCandidates() {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    default AlarmImpactView groupAlarmImpact(String groupId) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    default AlarmImpactView userAlarmImpact(String userId) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    default List<AlarmReceiverCandidates.CandidateGroup> alarmStats() {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    void ingestTaskAlert(TaskAlertRequest request);
}
