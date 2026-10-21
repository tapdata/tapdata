package com.tapdata.tm.notification.delivery.service;

import com.tapdata.tm.notification.delivery.dto.DeliverySummaryVo;
import com.tapdata.tm.notification.delivery.entity.NotificationDelivery;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface NotificationDeliveryService {

    /** 记录失败只告警日志，不影响通知发送本身 */
    void record(List<NotificationDelivery> deliveries);

    List<NotificationDelivery> findByAlarmEventId(String alarmEventId);

    /** 一次查询多个告警事件的投递记录 */
    List<NotificationDelivery> findByAlarmEventIds(Collection<String> alarmEventIds);

    List<NotificationDelivery> findByMessageIds(Collection<String> messageIds);

    Map<String, DeliverySummaryVo> summarizeByAlarmEventIds(Collection<String> alarmEventIds);

    DeliverySummaryVo summarize(Collection<NotificationDelivery> deliveries);
}
