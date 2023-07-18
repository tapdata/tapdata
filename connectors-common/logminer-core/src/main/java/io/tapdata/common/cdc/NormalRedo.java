package io.tapdata.common.cdc;

import net.openhft.chronicle.wire.Marshallable;

import java.io.Serializable;
import java.util.Map;

public class NormalRedo implements Serializable, Marshallable {

    private Long cdcSequenceId;
    private String cdcSequenceStr;
    private String operation;
    private Long timestamp;
    private String sqlRedo;
    private String sqlUndo;
    private String tableName;
    private String transactionId;
    private String nameSpace;
    private Map<String, Object> redoRecord;
    private Map<String, Object> undoRecord;

    public NormalRedo() {
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Long getCdcSequenceId() {
        return cdcSequenceId;
    }

    public void setCdcSequenceId(Long cdcSequenceId) {
        this.cdcSequenceId = cdcSequenceId;
    }

    public String getCdcSequenceStr() {
        return cdcSequenceStr;
    }

    public void setCdcSequenceStr(String cdcSequenceStr) {
        this.cdcSequenceStr = cdcSequenceStr;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public String getSqlRedo() {
        return sqlRedo;
    }

    public void setSqlRedo(String sqlRedo) {
        this.sqlRedo = sqlRedo;
    }

    public String getSqlUndo() {
        return sqlUndo;
    }

    public void setSqlUndo(String sqlUndo) {
        this.sqlUndo = sqlUndo;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, Object> getRedoRecord() {
        return redoRecord;
    }

    public void setRedoRecord(Map<String, Object> redoRecord) {
        this.redoRecord = redoRecord;
    }

    public Map<String, Object> getUndoRecord() {
        return undoRecord;
    }

    public void setUndoRecord(Map<String, Object> undoRecord) {
        this.undoRecord = undoRecord;
    }

    public enum OperationEnum {
        INSERT,
        DELETE,
        UPDATE,
        COMMIT,
        DDL,
        ROLLBACK,
        BEGIN,
        ;
    }
}
