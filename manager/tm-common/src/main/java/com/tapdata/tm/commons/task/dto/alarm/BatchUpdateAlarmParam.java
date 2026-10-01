package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/13 17:57 Create
 * @description
 */
@Data
public class BatchUpdateAlarmParam {
    List<String> taskIds;
    private List<AlarmSettingVO> alarmSettings;
    private List<AlarmRuleVO> alarmRules;
    private List<String> emailReceivers;
    /** null 表示本次不修改接收对象 */
    private List<AlarmReceiver> alarmReceivers;
    /** 仅 alarmReceivers 非 null 时有效，缺省按追加 */
    private ReceiverBatchMode receiverMode;
}
