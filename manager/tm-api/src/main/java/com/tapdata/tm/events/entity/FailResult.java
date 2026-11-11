package com.tapdata.tm.events.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

@Data
@NoArgsConstructor
public class FailResult {
    private Long retry;
    private Long next_retry;
    private String fail_message;
    private Long ts;

    /**
     * 部分送达时已收到通知的地址。
     *
     * <p>重试时需从收件人中排除，避免已送达的人重复收到同一封通知。
     * 整体失败或无此信息时为空列表。
     */
    private List<String> delivered_addresses;

    public FailResult(Long retry, Long next_retry, String fail_message, Long ts) {
        this(retry, next_retry, fail_message, ts, null);
    }

    public FailResult(Long retry, Long next_retry, String fail_message, Long ts, List<String> delivered_addresses) {
        this.retry = retry;
        this.next_retry = next_retry;
        this.fail_message = fail_message;
        this.ts = ts;
        this.delivered_addresses = delivered_addresses;
    }

    public List<String> getDelivered_addresses() {
        return delivered_addresses == null ? Collections.emptyList() : delivered_addresses;
    }
}
