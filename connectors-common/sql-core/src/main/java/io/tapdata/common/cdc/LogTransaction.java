package io.tapdata.common.cdc;

import io.tapdata.kit.EmptyKit;

import java.util.*;

/**
 * Created by tapdata on 23/03/2018.
 */
public class LogTransaction {

    public static final String TX_TYPE_DDL = "ddl";
    public static final String TX_TYPE_DML = "dml";
    public static final String TX_TYPE_COMMIT = "commit";
    public static final long LARGE_TRANSACTION_UPPER_LIMIT = 1000L;

    /**
     * transaction first rs id
     */
    private String rsId;

    private long scn;

    private String xid;

    private String transactionType = TX_TYPE_DML;

    /**
     * key: rs id
     * value: same rs id redo log event
     */
    private Map<String, List<RedoLogContent>> redoLogContents;

    private long size;

    private Set<String> txUpdatedRowIds = new HashSet<>();

    private Long racMinimalScn;

    private Long firstTimestamp;

    private boolean hasRollback;

    private int rollbackTemp;

    private Long lastTimestamp;

    private Long lastCommitTimestamp;

    private long receivedCommitTs;

    public LogTransaction(String rsId, long scn, String xid, Map<String, List<RedoLogContent>> redoLogContents) {
        this.rsId = rsId;
        this.scn = scn;
        this.xid = xid;
        this.redoLogContents = redoLogContents;
    }

    public LogTransaction(String rsId, long scn, String xid, Map<String, List<RedoLogContent>> redoLogContents, Long firstTimestamp) {
        this.rsId = rsId;
        this.scn = scn;
        this.xid = xid;
        this.redoLogContents = redoLogContents;
        this.firstTimestamp = firstTimestamp;
    }

    public void addRedoLogContent(RedoLogContent redoLogContent) {
        String rsId = redoLogContent.getRsId();
        if (redoLogContents == null) {
            redoLogContents = new LinkedHashMap<>();
        }

        if (!redoLogContents.containsKey(rsId)) {
            redoLogContents.put(rsId, new ArrayList<>());
        }

        redoLogContents.get(rsId).add(redoLogContent);
        if ("UPDATE".equals(redoLogContent.getOperation())) {
            txUpdatedRowIds.add(redoLogContent.getRowId());
        }
    }

    public void clearRedoLogContents() {
        if (EmptyKit.isNotEmpty(redoLogContents)) {
            redoLogContents.clear();
            txUpdatedRowIds.clear();
        }
    }

    public Long getRacMinimalScn() {
        return racMinimalScn;
    }

    public void setRacMinimalScn(Long racMinimalScn) {
        this.racMinimalScn = racMinimalScn;
    }

    public String getRsId() {
        return rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
    }

    public long getScn() {
        return scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public String getXid() {
        return xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }

    public Map<String, List<RedoLogContent>> getRedoLogContents() {
        return redoLogContents;
    }

    public void setRedoLogContents(Map<String, List<RedoLogContent>> redoLogContents) {
        this.redoLogContents = redoLogContents;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public long getSize() {
        return size;
    }

    public void incrementSize(int delta) {
        this.size += delta;
    }

    public boolean isLarge() {
        return this.size > LARGE_TRANSACTION_UPPER_LIMIT;
    }

    public Set<String> getTxUpdatedRowIds() {
        return txUpdatedRowIds;
    }

    public void setTxUpdatedRowIds(Set<String> txUpdatedRowIds) {
        this.txUpdatedRowIds = txUpdatedRowIds;
    }

    public Long getFirstTimestamp() {
        return firstTimestamp;
    }

    public int getRollbackTemp() {
        return rollbackTemp;
    }

    public void setRollbackTemp(int rollbackTemp) {
        this.rollbackTemp = rollbackTemp;
    }

    public boolean isHasRollback() {
        return hasRollback;
    }

    public void setHasRollback(boolean hasRollback) {
        this.hasRollback = hasRollback;
    }

    public Long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(Long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    public Long getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    public void setLastCommitTimestamp(Long lastCommitTimestamp) {
        this.lastCommitTimestamp = lastCommitTimestamp;
    }

    public long getReceivedCommitTs() {
        return receivedCommitTs;
    }

    public void setReceivedCommitTs(long receivedCommitTs) {
        this.receivedCommitTs = receivedCommitTs;
    }

    @Override
    public String toString() {
        return "LogTransaction{" + "rsId='" + rsId + '\'' +
                ", scn=" + scn +
                ", xid='" + xid + '\'' +
                ", redoLogContents=" + redoLogContents +
                '}';
    }
}
