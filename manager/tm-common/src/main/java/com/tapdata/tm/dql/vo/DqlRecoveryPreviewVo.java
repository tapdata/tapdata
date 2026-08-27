package com.tapdata.tm.dql.vo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
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
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class OrderedEvent extends DqlEventDetailVo implements Serializable {
        private String status;
        private Boolean overwriteRisk;
        private String overwriteRiskMessage;
        private Date laterSuccessAt;
        private Date laterSuccessEventTime;
        private Long laterSuccessCaptureSeq;
        private String laterSuccessDmlType;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class BlockedEvent implements Serializable {
        private String eventId;
        private String message;
        private String sourceTable;
        private String targetTable;
        private String dmlType;
        private Date eventTime;
        private Long captureSeq;

        /**
         * Kept as a source-compatible alias for callers compiled against the pre-B12 VO.
         * The JSON contract uses {@code message}.
         */
        @JsonIgnore
        public String getReason() {
            return message;
        }

        /**
         * Kept as a source-compatible alias for callers compiled against the pre-B12 VO.
         */
        @JsonIgnore
        public void setReason(String reason) {
            this.message = reason;
        }
    }
}
