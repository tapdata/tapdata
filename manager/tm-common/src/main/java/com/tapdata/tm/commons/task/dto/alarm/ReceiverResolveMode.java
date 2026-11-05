package com.tapdata.tm.commons.task.dto.alarm;

/**
 * 发送前解析出的接收人语义。null 与空列表不是同一种模式。
 */
public enum ReceiverResolveMode {
    /** 未配置 alarmReceivers，且没有存量邮箱，走系统默认收件人 */
    DEFAULT,
    /** 未配置 alarmReceivers，沿用存量 emailReceivers */
    LEGACY,
    /** 显式配置了 alarmReceivers，包括空列表 */
    CUSTOM
}
