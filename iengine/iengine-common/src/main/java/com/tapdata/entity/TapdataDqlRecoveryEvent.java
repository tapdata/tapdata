package com.tapdata.entity;

import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A DQL recovery event that carries one persisted TapRecordEvent through the
 * normal source-to-target processing graph.
 *
 * <p>The inherited TapdataEvent eventId is used for the TM DQL event id. A
 * second event-id field would shadow the parent field and would be lost by
 * the parent clone implementation.</p>
 */
public class TapdataDqlRecoveryEvent extends TapdataEvent {
    public static final String INFO_KEY_DQL_RECOVERY = "DQL_RECOVERY";
    public static final String INFO_KEY_DQL_EVENT_ID = "DQL_EVENT_ID";
    public static final String INFO_KEY_DQL_BATCH_ID = "DQL_BATCH_ID";
    public static final String INFO_KEY_DQL_ATTEMPT_ID = "DQL_ATTEMPT_ID";

    public static final String TYPE_BEGIN = "BEGIN";
    public static final String TYPE_DATA = "DATA";
    public static final String TYPE_END = "END";

    private String batchId;
    private String attemptId;
    private String recoveryType;
    private String operatorId;
    private Long taskVersion;

    public TapdataDqlRecoveryEvent() {
        super.setSyncStage(SyncStage.CDC);
    }

    private TapdataDqlRecoveryEvent(String batchId, String recoveryType) {
        this();
        this.batchId = batchId;
        this.recoveryType = recoveryType;
    }

    public static TapdataDqlRecoveryEvent createBegin(String batchId) {
        return new TapdataDqlRecoveryEvent(batchId, TYPE_BEGIN);
    }

    public static TapdataDqlRecoveryEvent createData(String batchId,
                                                     String eventId,
                                                     String attemptId,
                                                     String operatorId,
                                                     Long taskVersion,
                                                     DqlPayloadSnapshot snapshot) {
        return createData(batchId, eventId, attemptId, operatorId, taskVersion, snapshot,
                DqlRuntimeConfig.defaults());
    }

    public static TapdataDqlRecoveryEvent createData(String batchId,
                                                     String eventId,
                                                     String attemptId,
                                                     String operatorId,
                                                     Long taskVersion,
                                                     DqlPayloadSnapshot snapshot,
                                                     DqlRuntimeConfig config) {
        TapdataDqlRecoveryEvent recoveryEvent = new TapdataDqlRecoveryEvent(batchId, TYPE_DATA);
        recoveryEvent.setEventId(eventId);
        recoveryEvent.attemptId = attemptId;
        recoveryEvent.operatorId = operatorId;
        recoveryEvent.taskVersion = taskVersion;

        TapRecordEvent tapRecordEvent = new DqlPayloadSerializer(config).deserialize(snapshot);
        Map<String, Object> info = tapRecordEvent.getInfo() == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(tapRecordEvent.getInfo());
        info.put(INFO_KEY_DQL_RECOVERY, Boolean.TRUE);
        info.put(INFO_KEY_DQL_EVENT_ID, eventId);
        info.put(INFO_KEY_DQL_BATCH_ID, batchId);
        info.put(INFO_KEY_DQL_ATTEMPT_ID, attemptId);
        tapRecordEvent.setInfo(info);
        recoveryEvent.setTapEvent(tapRecordEvent);
        return recoveryEvent;
    }

    public static TapdataDqlRecoveryEvent createEnd(String batchId) {
        return new TapdataDqlRecoveryEvent(batchId, TYPE_END);
    }

    public boolean isDataEvent() {
        return TYPE_DATA.equals(recoveryType);
    }

    public static boolean isRecoveryEvent(TapEvent tapEvent) {
        return tapEvent != null
                && Boolean.TRUE.equals(tapEvent.getInfo() == null
                ? null
                : tapEvent.getInfo().get(INFO_KEY_DQL_RECOVERY));
    }

    @Override
    public boolean isConcurrentWrite() {
        return isDataEvent();
    }

    @Override
    protected void clone(TapdataEvent tapdataEvent) {
        super.clone(tapdataEvent);
        if (tapdataEvent instanceof TapdataDqlRecoveryEvent recoveryEvent) {
            recoveryEvent.batchId = batchId;
            recoveryEvent.attemptId = attemptId;
            recoveryEvent.recoveryType = recoveryType;
            recoveryEvent.operatorId = operatorId;
            recoveryEvent.taskVersion = taskVersion;
        }
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getAttemptId() {
        return attemptId;
    }

    public void setAttemptId(String attemptId) {
        this.attemptId = attemptId;
    }

    public String getRecoveryType() {
        return recoveryType;
    }

    public void setRecoveryType(String recoveryType) {
        this.recoveryType = recoveryType;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }

    public Long getTaskVersion() {
        return taskVersion;
    }

    public void setTaskVersion(Long taskVersion) {
        this.taskVersion = taskVersion;
    }
}
