package com.tapdata.tm.notification.delivery.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.notification.delivery.constant.DeliveryChannel;
import com.tapdata.tm.notification.delivery.constant.DeliveryStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * 告警通知按渠道、收件人记录的投递事实；地址为发送时快照，不随任务配置变化。
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Document("NotificationDelivery")
@CompoundIndexes({
        @CompoundIndex(name = "alarm_channel_status", def = "{'alarmEventId': 1, 'channel': 1, 'status': 1}"),
        @CompoundIndex(name = "message_channel_status", def = "{'messageId': 1, 'channel': 1, 'status': 1}"),
        @CompoundIndex(name = "recipient_created", def = "{'recipientAddress': 1, 'createTime': -1}")
})
public class NotificationDelivery extends BaseEntity {
    /** 对应的站内消息，邮件等外部渠道可为空 */
    private String messageId;
    private String alarmEventId;
    /** 同一次通知调度的批次 */
    private String batchId;
    private DeliveryChannel channel;
    private String recipientAddress;
    private String recipientUserId;
    private DeliveryStatus status;
    private Date sentAt;
    private String failureMessage;
    private String skipReason;
    /** 发送时告警的发生次数 */
    private Integer occurrenceCountSnapshot;
}
