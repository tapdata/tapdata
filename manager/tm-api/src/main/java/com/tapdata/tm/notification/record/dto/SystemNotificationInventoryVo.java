package com.tapdata.tm.notification.record.dto;

import com.tapdata.tm.message.constant.SystemNotificationMapping;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 历史系统通知盘点（只读）：按消息类型统计数量及其新归属。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SystemNotificationInventoryVo {
    private String msg;
    private SystemNotificationMapping.Target target;
    private long count;
    private long unread;
}
