package io.tapdata.common.cdc;

import io.tapdata.kit.DateTimeKit;
import io.tapdata.kit.EmptyKit;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.*;

/**
 * Created by tapdata on 08/12/2017.
 */
public class RedoLogContent implements Serializable {

    private final static String ID_DELIM = "-";

    private Boolean isGrpc = false;

    private long scn;

    private String scnStr;

    /**
     * INSERT = change was caused by an insert statement
     * <p>
     * UPDATE = change was caused by an update statement
     * <p>
     * DELETE = change was caused by a delete statement
     * <p>
     * DDL = change was caused by a DDL statement
     * <p>
     * START = change was caused by the start of a transaction
     * <p>
     * COMMIT = change was caused by the commit of a transaction
     * <p>
     * ROLLBACK = change was caused by a full rollback of a transaction
     * <p>
     * LOB_WRITE = change was caused by an invocation of DBMS_LOB.WRITE
     * <p>
     * LOB_TRIM = change was caused by an invocation of DBMS_LOB.TRIM
     * <p>
     * LOB_ERASE = change was caused by an invocation of DBMS_LOB.ERASE
     * <p>
     * SELECT_FOR_UPDATE = operation was a SELECT FOR UPDATE statement
     * <p>
     * SEL_LOB_LOCATOR = operation was a SELECT statement that returns a LOB locator
     * <p>
     * MISSING_SCN = LogMiner encountered a gap in the redo records. This is most likely because not all redo logs were registered with LogMiner.
     * <p>
     * INTERNAL = change was caused by internal operations initiated by the database
     * <p>
     * UNSUPPORTED = change was caused by operations not currently supported by LogMiner (for example, changes made to tables with ADT columns)
     */
    private String operation;

    private int operationCode;

    private Timestamp timestamp;

    private Long status;

    private String sqlRedo;

    private String sqlUndo;

    private String rowId;

    private String tableName;

    private String rsId;

    private Long ssn;

    private String xid;

    private String segOwner;

    /**
     * Is redo/undo sql split
     * 1 - split sql
     * 0 - complete sql
     */
    private int csf;

    private Set<String> lobWriteWhere; // LOB_WRITE 事件反查条件，理论上来说只有1个结果

    private String undoOperation;

    private int undoOperationCode;

    private int rollback;

    private int thread;

    private Timestamp commitTimestamp;

    private String info;

    private Map<String, Object> redoRecord;

    private Map<String, Object> undoRecord;

    public RedoLogContent() {
    }

    public RedoLogContent(ResultSet resultSet, ZoneId sysTimezone) throws SQLException {
        this.scn = resultSet.getLong(1);
        this.operation = resultSet.getString(2);
        this.timestamp = DateTimeKit.convertRedoContentTimestamp(resultSet.getTimestamp(3), sysTimezone);
        this.status = resultSet.getLong(4);
        this.sqlRedo = resultSet.getString(5);
        this.sqlUndo = resultSet.getString(6);
        this.rowId = resultSet.getString(7);
        this.tableName = resultSet.getString(8);
        this.rsId = resultSet.getString(9);
        this.ssn = resultSet.getLong(10);
        this.xid = resultSet.getString(11);
        this.operationCode = resultSet.getInt(12);
        this.segOwner = resultSet.getString(13);
        this.rollback = resultSet.getInt("ROLLBACK");
        this.thread = resultSet.getInt("THREAD#");
        this.info = resultSet.getString("INFO");

        if (EmptyKit.isBlank(operation)) {
            setOperationFromOperationCode();
        }
    }

    public RedoLogContent(ResultSet resultSet, Map<Long, String> tableObjectId, ZoneId sysTimezone) throws SQLException {
        this.scn = resultSet.getLong(1);
        this.operation = resultSet.getString(2);
        this.timestamp = DateTimeKit.convertRedoContentTimestamp(resultSet.getTimestamp(3), sysTimezone);
        this.status = resultSet.getLong(4);
        this.sqlRedo = resultSet.getString(5);
        this.sqlUndo = resultSet.getString(6);
        this.rowId = resultSet.getString(7);
        long objectId = resultSet.getLong(8);
        if (EmptyKit.isNotEmpty(tableObjectId)) {
            this.tableName = tableObjectId.get(objectId);
        }
        this.rsId = resultSet.getString(9);
        this.ssn = resultSet.getLong(10);
        this.xid = resultSet.getString(11);
        this.operationCode = resultSet.getInt(12);
        this.segOwner = resultSet.getString(13);
        this.rollback = resultSet.getInt("ROLLBACK");
        this.thread = resultSet.getInt("THREAD#");
        this.info = resultSet.getString("INFO");

        if (EmptyKit.isBlank(operation)) {
            setOperationFromOperationCode();
        }
    }

    public RedoLogContent(Map<String, Object> logData) {
        this.scn = logData.get("SCN") == null ? null : Long.valueOf(logData.get("SCN").toString());
        this.operation = logData.get("OPERATION") == null ? "" : logData.get("OPERATION").toString();
        this.timestamp = logData.get("TIMESTAMP") == null ? null : new Timestamp(((Date) logData.get("TIMESTAMP")).getTime());
        this.status = logData.get("STATUS") == null ? null : Long.valueOf(logData.get("STATUS").toString());
        this.sqlRedo = logData.get("SQL_REDO") == null ? "" : logData.get("SQL_REDO").toString();
        this.sqlUndo = logData.get("SQL_UNDO") == null ? "" : logData.get("SQL_UNDO").toString();
        this.rowId = logData.get("ROW_ID") == null ? "" : logData.get("ROW_ID").toString();
        this.tableName = logData.get("TABLE_NAME") == null ? "" : logData.get("TABLE_NAME").toString();
        this.rsId = logData.get("RS_ID") == null ? "" : logData.get("RS_ID").toString();
        this.ssn = logData.get("SSN") == null ? null : Long.valueOf(logData.get("SSN").toString());
        this.xid = logData.get("XID") == null ? "" : logData.get("XID").toString();
        this.operationCode = logData.get("OPERATION_CODE") == null ? null : Integer.valueOf(logData.get("OPERATION_CODE").toString());
        this.segOwner = logData.get("SEG_OWNER") == null ? "" : logData.get("SEG_OWNER").toString();
        this.rollback = logData.get("ROLLBACK") == null ? 0 : Integer.parseInt(logData.get("ROLLBACK").toString());
        this.thread = logData.get("THREAD#") == null ? 0 : Integer.parseInt(logData.get("THREAD#").toString());
        this.info = logData.get("INFO") == null ? "" : logData.get("INFO").toString();

        if (EmptyKit.isBlank(operation)) {
            setOperationFromOperationCode();
        }
    }

    public RedoLogContent(Map<String, Object> logData, Map<Long, String> tableObjectId) {
        this.scn = logData.get("SCN") == null ? null : Long.valueOf(logData.get("SCN").toString());
        this.operation = logData.get("OPERATION") == null ? "" : logData.get("OPERATION").toString();
        this.timestamp = logData.get("TIMESTAMP") == null ? null : new Timestamp(((Date) logData.get("TIMESTAMP")).getTime());
        this.status = logData.get("STATUS") == null ? null : Long.valueOf(logData.get("STATUS").toString());
        this.sqlRedo = logData.get("SQL_REDO") == null ? "" : logData.get("SQL_REDO").toString();
        this.sqlUndo = logData.get("SQL_UNDO") == null ? "" : logData.get("SQL_UNDO").toString();
        this.rowId = logData.get("ROW_ID") == null ? "" : logData.get("ROW_ID").toString();
        Long objectId = logData.get("DATA_OBJ#") == null ? null : Long.valueOf(logData.get("DATA_OBJ#").toString());
        if (EmptyKit.isNotEmpty(tableObjectId) && objectId != null) {
            this.tableName = tableObjectId.get(objectId);
        }
        this.rsId = logData.get("RS_ID") == null ? "" : logData.get("RS_ID").toString();
        this.ssn = logData.get("SSN") == null ? null : Long.valueOf(logData.get("SSN").toString());
        this.xid = logData.get("XID") == null ? "" : logData.get("XID").toString();
        this.operationCode = logData.get("OPERATION_CODE") == null ? null : Integer.valueOf(logData.get("OPERATION_CODE").toString());
        this.segOwner = logData.get("SEG_OWNER") == null ? "" : logData.get("SEG_OWNER").toString();
        this.rollback = logData.get("ROLLBACK") == null ? 0 : Integer.parseInt(logData.get("ROLLBACK").toString());
        this.thread = logData.get("THREAD#") == null ? 0 : Integer.parseInt(logData.get("THREAD#").toString());
        this.info = logData.get("INFO") == null ? "" : logData.get("INFO").toString();

        if (EmptyKit.isBlank(operation)) {
            setOperationFromOperationCode();
        }
    }

    public Boolean getGrpc() {
        return isGrpc;
    }

    public void setGrpc(Boolean grpc) {
        isGrpc = grpc;
    }

    public long getScn() {
        return scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public String getScnStr() {
        return scnStr;
    }

    public void setScnStr(String scnStr) {
        this.scnStr = scnStr;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public Long getStatus() {
        return status;
    }

    public void setStatus(Long status) {
        this.status = status;
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

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRsId() {
        return rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
    }

    public Long getSsn() {
        return ssn;
    }

    public void setSsn(Long ssn) {
        this.ssn = ssn;
    }

    public String getXid() {
        return xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }

    public int getOperationCode() {
        return operationCode;
    }

    public void setOperationCode(int operationCode) {
        this.operationCode = operationCode;
    }

    public String getSegOwner() {
        return segOwner;
    }

    public void setSegOwner(String segOwner) {
        this.segOwner = segOwner;
    }

    public String getUndoOperation() {
        return undoOperation;
    }

    public void setOperationFromOperationCode() {
        OperationEnum operationEnum = OperationEnum.fromOperationCode(operationCode);
        if (operationEnum != null) {
            this.operation = operationEnum.getOperation();
        }
    }

    public int getUndoOperationCode() {
        return undoOperationCode;
    }

    public int getRollback() {
        return rollback;
    }

    public void setRollback(int rollback) {
        this.rollback = rollback;
    }

    public int getThread() {
        return thread;
    }

    public void setThread(int thread) {
        this.thread = thread;
    }

    public Timestamp getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(Timestamp commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
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

    public void setUndoOperationFromRedoOperation() {
        if (EmptyKit.isNotBlank(operation)) {
            OperationEnum operationEnum = OperationEnum.fromOperation(operation);
            OperationEnum undoOperationEnum = null;
            switch (operationEnum) {
                case INSERT:
                    undoOperationEnum = OperationEnum.DELETE;
                    break;
                case UPDATE:
                    undoOperationEnum = OperationEnum.UPDATE;
                    break;
                case DELETE:
                    undoOperationEnum = OperationEnum.INSERT;
                    break;
                default:
                    break;
            }
            if (undoOperationEnum != null) {
                this.undoOperation = undoOperationEnum.getOperation();
                this.undoOperationCode = undoOperationEnum.getCode();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedoLogContent that = (RedoLogContent) o;
        return scn == that.scn && thread == that.thread && rsId.equals(that.rsId) && ssn.equals(that.ssn) && xid.equals(that.xid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scn, rsId, ssn, xid, thread);
    }

    public static String id(Integer thread, String xid, Long scn, String rsId, Long ssn, Integer csf, String sqlRedo) {
        return thread + ID_DELIM +
                xid + ID_DELIM +
                scn + ID_DELIM +
                rsId + ID_DELIM +
                ssn + ID_DELIM +
                csf + ID_DELIM +
                sqlRedo;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    @Override
    public String toString() {
        return "RedoLogContent{" + "scn=" + scn +
                ", operation='" + operation + '\'' +
                ", operationCode=" + operationCode +
                ", timestamp=" + timestamp +
                ", status=" + status +
                ", sqlRedo='" + sqlRedo + '\'' +
                ", sqlUndo='" + sqlUndo + '\'' +
                ", rowId='" + rowId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", rsId='" + rsId + '\'' +
                ", ssn=" + ssn +
                ", xid='" + xid + '\'' +
                ", segOwner='" + segOwner + '\'' +
                ", csf=" + csf +
                ", undoOperation='" + undoOperation + '\'' +
                ", undoOperationCode=" + undoOperationCode +
                ", rollback=" + rollback +
                ", thread=" + thread +
                ", info='" + info + '\'' +
                '}';
    }

    public enum OperationEnum {
        INSERT("INSERT", 1, 2),
        DELETE("DELETE", 2, 4),
        UPDATE("UPDATE", 3, 3),
        COMMIT("COMMIT", 7, 1),
        SEL_LOB_LOCATOR("SEL_LOB_LOCATOR", 9, 999),
        SELECT_FOR_UPDATE("SELECT_FOR_UPDATE", 25, 998),
        DDL("DDL", 5, 5),
        ROLLBACK("ROLLBACK", 36, 997),
        CHKPT("CHKPT", 996, 6),
        BEGIN("BEGIN", 995, 0),
        ;

        private final String operation;
        private final int code;
        /**
         * 裸日志解析的code
         */
        private int redologCode;

        OperationEnum(String operation, int code) {
            this.operation = operation;
            this.code = code;
        }

        OperationEnum(String operation, int code, int redologCode) {
            this.operation = operation;
            this.code = code;
            this.redologCode = redologCode;
        }


        public String getOperation() {
            return operation;
        }

        public int getCode() {
            return code;
        }

        public int getRedologCode() {
            return redologCode;
        }

        private static final Map<String, OperationEnum> operationMap = new HashMap<>();
        private static final Map<Integer, OperationEnum> operationCodeMap = new HashMap<>();
        private static final Map<Integer, OperationEnum> redologOperationCodeMap = new HashMap<>();

        static {
            for (OperationEnum value : OperationEnum.values()) {
                operationMap.put(value.operation, value);
            }

            for (OperationEnum value : OperationEnum.values()) {
                operationCodeMap.put(value.code, value);
            }

            for (OperationEnum value : OperationEnum.values()) {
                redologOperationCodeMap.put(value.getRedologCode(), value);
            }
        }

        public static OperationEnum fromOperation(String operation) {
            return operationMap.get(operation);
        }

        public static OperationEnum fromOperationCode(int operationCode) {
            return operationCodeMap.get(operationCode);
        }

        public static OperationEnum fromRedologOperationCode(int redologCode) {
            return redologOperationCodeMap.get(redologCode);
        }
    }

    public int getCsf() {
        return csf;
    }

    public void setCsf(int csf) {
        this.csf = csf;
    }

    public Set<String> getLobWriteWhere() {
        return lobWriteWhere;
    }

    public void setLobWriteWhere(Set<String> lobWriteWhere) {
        this.lobWriteWhere = lobWriteWhere;
    }
}
