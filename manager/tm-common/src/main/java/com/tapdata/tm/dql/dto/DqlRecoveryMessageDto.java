package com.tapdata.tm.dql.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * TM -> Engine command for one DQL recovery batch.
 *
 * <p>This message deliberately has its own contract instead of reusing
 * {@code DataSyncMq.opType}; task lifecycle messages and recovery commands
 * have different state machines and retry semantics.</p>
 */
@Data
public class DqlRecoveryMessageDto implements Serializable {
    public static final String TYPE = "dqlRecovery";
    public static final String MODE_AUTO = "AUTO";

    private String type = TYPE;
    private String taskId;
    private String batchId;
    private Long taskVersion;
    private List<String> orderedEventIds;
    private String operatorId;
    private String operatorName;
    private String mode = MODE_AUTO;

    public static DqlRecoveryMessageDto fromBatch(DqlRecoveryBatchDto batch) {
        Objects.requireNonNull(batch, "batch");
        DqlRecoveryMessageDto message = new DqlRecoveryMessageDto();
        message.setTaskId(batch.getTaskId());
        message.setBatchId(batch.getBatchId());
        message.setTaskVersion(batch.getTaskVersion());
        message.setOrderedEventIds(batch.getOrderedEventIds());
        message.setOperatorId(batch.getOperatorId());
        message.setOperatorName(batch.getOperatorName());
        message.setMode(Objects.requireNonNullElse(batch.getMode(), MODE_AUTO));
        return message;
    }

    /**
     * Converts the typed contract to the map accepted by the existing pipe
     * message transport. The insertion order is kept stable for logs and
     * contract snapshots.
     */
    public Map<String, Object> toPayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", type);
        payload.put("taskId", taskId);
        payload.put("batchId", batchId);
        payload.put("taskVersion", taskVersion);
        payload.put("orderedEventIds", orderedEventIds);
        payload.put("operatorId", operatorId);
        payload.put("operatorName", operatorName);
        payload.put("mode", mode);
        return payload;
    }
}
