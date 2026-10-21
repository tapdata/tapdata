package com.tapdata.tm.notification.record.dto;

import com.tapdata.tm.alarm.constant.AlarmObjectType;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.notification.delivery.dto.DeliverySummaryVo;
import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 通知记录列表行：同一告警事件对当前用户的全部通知聚合为一行。
 */
@Data
public class NotificationRecordVo {
    /** 告警事件 ID；无关联告警的历史消息为 "msg:" + 消息 ID */
    private String id;
    private String alarmEventId;
    /** 无告警关联的历史通知，没有投递明细 */
    private boolean legacy;
    /** 关联的告警事件已不存在（历史版本在任务重新运行时删除），只能展示通知自身保存的信息 */
    private boolean alarmEventMissing;
    /** 组内全部站内消息 ID，仅详情返回；列表行为空 */
    private List<String> messageIds = new ArrayList<>();
    private String title;
    private String level;
    private AlarmKeyEnum metric;
    private AlarmStatusEnum alarmStatus;
    private AlarmObjectType objectType;
    private String taskId;
    private String taskName;
    private Date firstOccurrenceTime;
    private Date lastOccurrenceTime;
    private Integer occurrenceCount;
    /** 该告警已发出的站内通知条数 */
    private int notificationCount;
    /** 当前用户自己的消息是否有未读；他人的记录始终为 false */
    private boolean unread;
    /** 记录中有发给当前用户的消息，才可标记已读/未读 */
    private boolean readable;
    private Date latestNotifyTime;
    private DeliverySummaryVo deliverySummary;
}
