package com.tapdata.entity;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.utils.VfsFilepath;
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

    private String inspectTaskId; // 校验任务编号（单表）
    private String rowId; // 修复事件行编号
    private String recoveryType; // 修复事件类型

    private String manualId; // 手动操作标识（空为：自动修复）
    private String recoverySql; // 修复 SQL，仅用于导出
    private String recoverySqlFile; // 修复 SQL 导出路径

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
        return isDataEvent();
    }

    public boolean isDataEvent() {
        return RECOVERY_TYPE_DATA.equals(recoveryType);
    }

    public TapdataRecoveryEvent ofManual(String manualId) {
        setManualId(manualId);
        return this;
    }

    public TapdataRecoveryEvent ofTaskInspectRecoverSql(String taskId, String manualId) {
        String filepath = VfsFilepath.task_recoverSql_taskInspect(taskId, manualId);

        setManualId(manualId);
        setRecoverySqlFile(filepath);
        return this;
    }

    public TapdataRecoveryEvent ofInspectRecoverSql(boolean isExport, String taskId, String inspectResultId) {
        setManualId(inspectResultId);
        if (isExport) {
            String filepath = VfsFilepath.task_recoverSql_inspect(taskId, inspectResultId);
            setRecoverySqlFile(filepath);
        }
        return this;
    }

    public static TapdataRecoveryEvent createBegin(String inspectTaskId) {
        return new TapdataRecoveryEvent(inspectTaskId, RECOVERY_TYPE_BEGIN);
    }

    public static TapdataRecoveryEvent createInsert(String inspectTaskId, String tableId, String rowId, Map<String, Object> after) {
        TapdataRecoveryEvent tapdataEvent = new TapdataRecoveryEvent(inspectTaskId, RECOVERY_TYPE_DATA);
        TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().after(after).table(tableId);
        insertRecordEvent.addInfo(EVENT_INFO_AUTO_RECOVERY_TASK, inspectTaskId);
        tapdataEvent.setRowId(rowId);
        tapdataEvent.setTapEvent(insertRecordEvent);
        return tapdataEvent;
    }

    public static TapdataRecoveryEvent createDelete(String inspectTaskId, String tableId, String rowId, Map<String, Object> before) {
        TapdataRecoveryEvent tapdataEvent = new TapdataRecoveryEvent(inspectTaskId, RECOVERY_TYPE_DATA);
        TapDeleteRecordEvent deleteRecordEvent = TapDeleteRecordEvent.create().before(before).table(tableId);
        deleteRecordEvent.addInfo(EVENT_INFO_AUTO_RECOVERY_TASK, inspectTaskId);
        tapdataEvent.setRowId(rowId);
        tapdataEvent.setTapEvent(deleteRecordEvent);
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
