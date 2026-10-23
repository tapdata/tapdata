package com.tapdata.tm.notification.record.dto;

import com.tapdata.tm.notification.delivery.constant.DeliveryChannel;
import com.tapdata.tm.notification.delivery.constant.DeliveryStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class NotificationRecordDetailVo extends NotificationRecordVo {
    private String content;
    private Date readAt;
    private List<Batch> batches = new ArrayList<>();

    @Data
    public static class Batch {
        private String batchId;
        private Date time;
        /** 发送时告警的发生次数 */
        private Integer occurrenceCountSnapshot;
        private List<Recipient> recipients = new ArrayList<>();
    }

    @Data
    public static class Recipient {
        private DeliveryChannel channel;
        private String address;
        private DeliveryStatus status;
        private Date sentAt;
        private String failureMessage;
        private String skipReason;
    }
}
