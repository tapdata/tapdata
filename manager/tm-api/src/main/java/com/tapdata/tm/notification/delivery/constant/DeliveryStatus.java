package com.tapdata.tm.notification.delivery.constant;

/**
 * 单个收件人的投递结果；P0 为同步发送，不存在待发送/发送中。
 */
public enum DeliveryStatus {
    SUCCESS,
    FAILED,
    SKIPPED
}
