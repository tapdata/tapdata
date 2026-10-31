package com.tapdata.tm.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

@Getter
@Setter
@NoArgsConstructor
public class SendStatus {

    private String status;
    private String errorMessage;

    /**
     * 本次群发中已实际送达的收件地址。
     *
     * <p>当状态为 false 且该集合非空时，表示部分送达后仍有地址需要重试；
     * 重试方应排除这些地址，避免已收到通知的人重复收到。
     * 整体成功或无法确定时为空集合。
     */
    private Set<String> deliveredAddresses;

    public SendStatus(String status, String errorMessage) {
        this(status, errorMessage, null);
    }

    public SendStatus(String status, String errorMessage, Set<String> deliveredAddresses) {
        this.status = status;
        this.errorMessage = errorMessage;
        this.deliveredAddresses = deliveredAddresses == null
                ? Collections.emptySet() : new LinkedHashSet<>(deliveredAddresses);
    }

    public Set<String> getDeliveredAddresses() {
        return deliveredAddresses == null ? Collections.emptySet() : deliveredAddresses;
    }
}
