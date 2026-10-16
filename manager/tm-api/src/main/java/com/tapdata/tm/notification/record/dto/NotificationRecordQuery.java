package com.tapdata.tm.notification.record.dto;

import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.notification.delivery.constant.DeliveryChannel;
import lombok.Data;

import java.util.List;

/**
 * 通知记录筛选。tab：ALL / UNREAD / FAILED。
 */
@Data
public class NotificationRecordQuery {
    public static final String TAB_ALL = "ALL";
    public static final String TAB_UNREAD = "UNREAD";
    public static final String TAB_FAILED = "FAILED";

    private String tab = TAB_ALL;
    /** 通知时间范围（毫秒） */
    private Long start;
    private Long end;
    private List<Level> level;
    /** 任务/对象名称或 ID */
    private String keyword;
    private List<AlarmKeyEnum> metric;
    private List<DeliveryChannel> channel;
    /** SUCCESS / PARTIAL_FAILED / FAILED / SKIPPED / NONE */
    private List<String> deliveryStatus;
    private String recipient;
    private Boolean read;
    /** 从告警事件详情跳转时按告警筛选 */
    private String alarmEventId;
    private Integer page = 1;
    private Integer size = 20;
}
