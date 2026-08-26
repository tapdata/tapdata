package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
public class DqlRecoveryPreviewVo implements Serializable {
    private String taskId;
    private String taskName;
    private boolean canSubmit;
    private List<OrderedEvent> orderedEvents = new ArrayList<>();
    private List<BlockedEvent> blockedEvents = new ArrayList<>();
    private String message = "";

    @Data
    public static class OrderedEvent implements Serializable {
        private String eventId;
        private Date eventTime;
        private Long captureSeq;
        private String dmlType;
        private String sourceTable;
    }

    @Data
    public static class BlockedEvent implements Serializable {
        private String eventId;
        private String reason;
    }
}
