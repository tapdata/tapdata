package com.tapdata.tm.notification.delivery.dto;

import com.tapdata.tm.notification.delivery.constant.DeliveryChannel;
import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 投递汇总：整体结果加上各渠道成功/总数，例如“站内成功 · 邮件 2/3”。
 */
@Data
public class DeliverySummaryVo {
    public static final String NONE = "NONE";
    public static final String SUCCESS = "SUCCESS";
    public static final String PARTIAL_FAILED = "PARTIAL_FAILED";
    public static final String FAILED = "FAILED";
    public static final String SKIPPED = "SKIPPED";

    private String status = NONE;
    private int total;
    private int success;
    private int failed;
    private int skipped;
    private List<DeliveryChannel> channels = new ArrayList<>();
    private Map<DeliveryChannel, ChannelSummary> byChannel = new LinkedHashMap<>();
    private Date latestDeliveryTime;

    @Data
    public static class ChannelSummary {
        private int total;
        private int success;
        private int failed;
        private int skipped;
    }
}
