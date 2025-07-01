package com.tapdata.entity;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * 修复事件
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/7 11:01 Create
 */
@Setter
@Getter
public class TapdataRecoveryEvent extends TapdataEvent {

    public static final String EVENT_INFO_AUTO_RECOVERY_TASK = "AUTO_RECOVERY_TASK";

    public static final String RECOVERY_TYPE_BEGIN = "BEGIN";
    public static final String RECOVERY_TYPE_DATA = "DATA";
    public static final String RECOVERY_TYPE_END = "END";

    private String inspectTaskId;
    private String rowId;
    private String recoveryType;

    private String recoverySql;

    private Boolean isExport = false;

    private String inspectResultId;

    private String inspectId;

    private Boolean isManual = false; // 是否手动校验

    public TapdataRecoveryEvent() {
        super.syncStage = SyncStage.CDC;
    }

    public TapdataRecoveryEvent(String inspectTaskId, String recoveryType) {
        super.syncStage = SyncStage.CDC;
        this.inspectTaskId = inspectTaskId;
        this.recoveryType = recoveryType;
    }

    @Override
    public boolean isConcurrentWrite() {
        return RECOVERY_TYPE_DATA.equals(recoveryType);
    }

    public static TapdataRecoveryEvent createBegin(String inspectTaskId) {
        return new TapdataRecoveryEvent(inspectTaskId, RECOVERY_TYPE_BEGIN);
    }

    public static TapdataRecoveryEvent createInsert(String inspectTaskId, String tableId, Map<String, Object> after,Boolean isExport,String inspectResultId,String inspectId) {
        TapdataRecoveryEvent tapdataEvent = new TapdataRecoveryEvent(inspectTaskId, RECOVERY_TYPE_DATA);
        tapdataEvent.setInspectResultId(inspectResultId);
        tapdataEvent.setInspectId(inspectId);
        tapdataEvent.setIsExport(isExport);
        TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().after(after).table(tableId);
        insertRecordEvent.addInfo(EVENT_INFO_AUTO_RECOVERY_TASK, inspectTaskId);
        tapdataEvent.setTapEvent(insertRecordEvent);
        return tapdataEvent;
    }

    public static TapdataRecoveryEvent createInsert(String inspectTaskId, boolean isManual, String rowId, String tableId, Map<String, Object> after) {
        TapdataRecoveryEvent tapdataEvent = new TapdataRecoveryEvent(inspectTaskId, RECOVERY_TYPE_DATA);
        TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().after(after).table(tableId);
        insertRecordEvent.addInfo(EVENT_INFO_AUTO_RECOVERY_TASK, inspectTaskId);
        tapdataEvent.setIsManual(isManual);
        tapdataEvent.setRowId(rowId);
        tapdataEvent.setTapEvent(insertRecordEvent);
        return tapdataEvent;
    }

    public static TapdataRecoveryEvent createEnd(String inspectTaskId) {
        return new TapdataRecoveryEvent(inspectTaskId, RECOVERY_TYPE_END);
    }

    public static boolean isRecoveryEvent(TapEvent tapEvent) {
        if (null != tapEvent.getInfo()) {
            return tapEvent.getInfo().containsKey(EVENT_INFO_AUTO_RECOVERY_TASK);
        }
        return false;
    }
}
