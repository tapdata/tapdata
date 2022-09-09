package io.tapdata.connector.dameng.cdc.logminer.constant;


public class LogmnrRecord {
    private long scn;

    private long startScn;

    private long commitScn;

    private String timestamp;

    private String startTimestamp;

    private String commitTimestamp;

    private long xidusn;

    private long xidslt;

    private long xidsqn;

    private String xid;

    private long pxidusn;

    private long pxidslt;

    private long pxidsqn;

    private String pxid;

    private String txName;

    private String operation;

    private int operationCode;

    private int rollBack;

    private String segOwner;

    private String segName;

    private String tableName;

    private int segType;

    private String segTypeName;

    private String tableSpace;

    private String rowId;

    private String userName;

    private String osUserName;

    private String machineName;

    private long auditSessionId;

    private long session;

    private long serial;

    private String sessionInfo;

    private long thread;

    private int sequance;

    private int rbasqn;

    private int rbablk;

    private int rbabyte;

    private long ubafil;

    private long ubablk;

    private long ubarec;

    private long ubasqn;

    private int absFile;

    private int relFile;

    private int dataBlk;

    private int dataObj;

    private int dataObjv;

    private int dataObjd;

    private String sqlRedo;

    private String sqlUndo;

    private String rsId;

    private int ssn;

    private int csf;

    private String info;

    private int status;

    private long redoValue;

    private long undoValue;

    private long safeResumeScn;

    private long cscn;

    private String objectId;

    private String editionName;

    private String clientId;

    public long getScn() {
        return this.scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public long getStartScn() {
        return this.startScn;
    }

    public void setStartScn(long startScn) {
        this.startScn = startScn;
    }

    public long getCommitScn() {
        return this.commitScn;
    }

    public void setCommitScn(long commitScn) {
        this.commitScn = commitScn;
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getStartTimestamp() {
        return this.startTimestamp;
    }

    public void setStartTimestamp(String startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public String getCommitTimestamp() {
        return this.commitTimestamp;
    }

    public void setCommitTimestamp(String commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public long getXidusn() {
        return this.xidusn;
    }

    public void setXidusn(long xidusn) {
        this.xidusn = xidusn;
    }

    public long getXidslt() {
        return this.xidslt;
    }

    public void setXidslt(long xidslt) {
        this.xidslt = xidslt;
    }

    public long getXidsqn() {
        return this.xidsqn;
    }

    public void setXidsqn(long xidsqn) {
        this.xidsqn = xidsqn;
    }

    public String getXid() {
        return this.xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }

    public long getPxidusn() {
        return this.pxidusn;
    }

    public void setPxidusn(long pxidusn) {
        this.pxidusn = pxidusn;
    }

    public long getPxidslt() {
        return this.pxidslt;
    }

    public void setPxidslt(long pxidslt) {
        this.pxidslt = pxidslt;
    }

    public long getPxidsqn() {
        return this.pxidsqn;
    }

    public void setPxidsqn(long pxidsqn) {
        this.pxidsqn = pxidsqn;
    }

    public String getPxid() {
        return this.pxid;
    }

    public void setPxid(String pxid) {
        this.pxid = pxid;
    }

    public String getTxName() {
        return this.txName;
    }

    public void setTxName(String txName) {
        this.txName = txName;
    }

    public String getOperation() {
        return this.operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public int getOperationCode() {
        return this.operationCode;
    }

    public void setOperationCode(int operationCode) {
        this.operationCode = operationCode;
    }

    public int getRollBack() {
        return this.rollBack;
    }

    public void setRollBack(int rollBack) {
        this.rollBack = rollBack;
    }

    public String getSegOwner() {
        return this.segOwner;
    }

    public void setSegOwner(String segOwner) {
        this.segOwner = segOwner;
    }

    public String getSegName() {
        return this.segName;
    }

    public void setSegName(String segName) {
        this.segName = segName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getSegTye() {
        return this.segType;
    }

    public void setSegTye(int segType) {
        this.segType = segType;
    }

    public String getSegTypeName() {
        return this.segTypeName;
    }

    public void setSegTypeName(String segTypeName) {
        this.segTypeName = segTypeName;
    }

    public String getTableSpace() {
        return this.tableSpace;
    }

    public void setTableSpace(String tableSpace) {
        this.tableSpace = tableSpace;
    }

    public String getRowId() {
        return this.rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public String getUserName() {
        return this.userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getOsUserName() {
        return this.osUserName;
    }

    public void setOsUserName(String osUserName) {
        this.osUserName = osUserName;
    }

    public String getMachineName() {
        return this.machineName;
    }

    public void setMachineName(String machineName) {
        this.machineName = machineName;
    }

    public long getAuditSessionId() {
        return this.auditSessionId;
    }

    public void setAuditSessionId(long auditSessionId) {
        this.auditSessionId = auditSessionId;
    }

    public long getSession() {
        return this.session;
    }

    public void setSession(long session) {
        this.session = session;
    }

    public long getSerial() {
        return this.serial;
    }

    public void setSerial(long serial) {
        this.serial = serial;
    }

    public String getSessionInfo() {
        return this.sessionInfo;
    }

    public void setSessionInfo(String sessionInfo) {
        this.sessionInfo = sessionInfo;
    }

    public long getThread() {
        return this.thread;
    }

    public void setThread(long thread) {
        this.thread = thread;
    }

    public int getSequance() {
        return this.sequance;
    }

    public void setSequance(int sequance) {
        this.sequance = sequance;
    }

    public int getRbasqn() {
        return this.rbasqn;
    }

    public void setRbasqn(int rbasqn) {
        this.rbasqn = rbasqn;
    }

    public int getRbablk() {
        return this.rbablk;
    }

    public void setRbablk(int rbablk) {
        this.rbablk = rbablk;
    }

    public int getRbabyte() {
        return this.rbabyte;
    }

    public void setRbabyte(int rbabyte) {
        this.rbabyte = rbabyte;
    }

    public long getUbafil() {
        return this.ubafil;
    }

    public void setUbafil(long ubafil) {
        this.ubafil = ubafil;
    }

    public long getUbablk() {
        return this.ubablk;
    }

    public void setUbablk(long ubablk) {
        this.ubablk = ubablk;
    }

    public long getUbarec() {
        return this.ubarec;
    }

    public void setUbarec(long ubarec) {
        this.ubarec = ubarec;
    }

    public long getUbasqn() {
        return this.ubasqn;
    }

    public void setUbasqn(long ubasqn) {
        this.ubasqn = ubasqn;
    }

    public int getAbsFile() {
        return this.absFile;
    }

    public void setAbsFile(int absFile) {
        this.absFile = absFile;
    }

    public int getRelFile() {
        return this.relFile;
    }

    public void setRelFile(int relFile) {
        this.relFile = relFile;
    }

    public int getDataBlk() {
        return this.dataBlk;
    }

    public void setDataBlk(int dataBlk) {
        this.dataBlk = dataBlk;
    }

    public int getDataObj() {
        return this.dataObj;
    }

    public void setDataObj(int dataObj) {
        this.dataObj = dataObj;
    }

    public int getDataObjv() {
        return this.dataObjv;
    }

    public void setDataObjv(int dataObjv) {
        this.dataObjv = dataObjv;
    }

    public int getDataObjd() {
        return this.dataObjd;
    }

    public void setDataObjd(int dataObjd) {
        this.dataObjd = dataObjd;
    }

    public String getSqlRedo() {
        return this.sqlRedo;
    }

    public void setSqlRedo(String sqlRedo) {
        this.sqlRedo = sqlRedo;
    }

    public String getSqlUndo() {
        return this.sqlUndo;
    }

    public void setSqlUndo(String sqlUndo) {
        this.sqlUndo = sqlUndo;
    }

    public String getRsId() {
        return this.rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
    }

    public int getSsn() {
        return this.ssn;
    }

    public void setSsn(int ssn) {
        this.ssn = ssn;
    }

    public int getCsf() {
        return this.csf;
    }

    public void setCsf(int csf) {
        this.csf = csf;
    }

    public String getInfo() {
        return this.info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public int getStatus() {
        return this.status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getRedoValue() {
        return this.redoValue;
    }

    public void setRedoValue(long redoValue) {
        this.redoValue = redoValue;
    }

    public long getUndoValue() {
        return this.undoValue;
    }

    public void setUndoValue(long undoValue) {
        this.undoValue = undoValue;
    }

    public long getSafeResumeScn() {
        return this.safeResumeScn;
    }

    public void setSafeResumeScn(long safeResumeScn) {
        this.safeResumeScn = safeResumeScn;
    }

    public long getCscn() {
        return this.cscn;
    }

    public void setCscn(long cscn) {
        this.cscn = cscn;
    }

    public String getObjectId() {
        return this.objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public String getEditionName() {
        return this.editionName;
    }

    public void setEditionName(String editionName) {
        this.editionName = editionName;
    }

    public String getClientId() {
        return this.clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
