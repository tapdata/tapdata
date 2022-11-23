package io.tapdata.common.cdc;

import io.tapdata.kit.EmptyKit;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by tapdata on 23/03/2018.
 */
public class LogTransaction {

    public static final String TX_TYPE_DDL = "ddl";
    public static final String TX_TYPE_DML = "dml";
    public static final String TX_TYPE_COMMIT = "commit";
    public static final long LARGE_TRANSACTION_UPPER_LIMIT = 1000L;

    private String connectorId;

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
    private Map<String, List> redoLogContents;

    private ChronicleMap<String, List> chronicleMap;

    private long size;

    private Long racMinimalScn;

    private Long firstTimestamp;

    private boolean hasRollback;

    private int rollbackTemp;

    private Long lastTimestamp;

    private Long lastCommitTimestamp;

    private long receivedCommitTs;

    public LogTransaction(String rsId, long scn, String xid, Map<String, List> redoLogContents) {
        this.rsId = rsId;
        this.scn = scn;
        this.xid = xid;
        this.redoLogContents = redoLogContents;
    }

    public LogTransaction(String rsId, long scn, String xid, Map<String, List> redoLogContents, Long firstTimestamp) {
        this.rsId = rsId;
        this.scn = scn;
        this.xid = xid;
        this.redoLogContents = redoLogContents;
        this.firstTimestamp = firstTimestamp;
    }

    public void setConnectorId(String connectorId) {
        this.connectorId = connectorId;
    }

    public void addRedoLogContent(RedoLogContent redoLogContent) throws IOException {
        String rsId = redoLogContent.getRsId();
        if (EmptyKit.isNull(redoLogContents)) {
            redoLogContents = new LinkedHashMap<>();
        }
        if (size >= 1000L) {
            if (EmptyKit.isNull(chronicleMap)) {
                File cacheDir = new File("cacheTransaction" + File.separator + connectorId);
                if (!cacheDir.exists()) {
                    cacheDir.mkdirs();
                }
                File cacheFile = new File("cacheTransaction" + File.separator + connectorId + File.separator + xid + ".data");
                if (!cacheFile.exists()) {
                    cacheFile.createNewFile();
                }
                chronicleMap = ChronicleMap
                        .of(String.class, List.class)
                        .name("xid" + xid)
                        .averageKey(xid)
                        .averageValue(Collections.singletonList(redoLogContent))
                        .entries(500000L)
                        .maxBloatFactor(200)
                        .createPersistedTo(cacheFile);
                chronicleMap.putAll(redoLogContents);
            }
            if (!chronicleMap.containsKey(rsId)) {
                chronicleMap.put(rsId, Collections.singletonList(redoLogContent));
            } else {
                List<RedoLogContent> list = chronicleMap.get(rsId);
                list.add(redoLogContent);
                chronicleMap.put(rsId, list);
            }
        } else {
            if (!redoLogContents.containsKey(rsId)) {
                redoLogContents.put(rsId, new ArrayList<>());
            }
            redoLogContents.get(rsId).add(redoLogContent);
        }
    }

    public void clearRedoLogContents() {
        if (EmptyKit.isNotEmpty(redoLogContents)) {
            redoLogContents.clear();
        }
        if (EmptyKit.isNotEmpty(chronicleMap)) {
            chronicleMap.clear();
            chronicleMap.close();
        }
        File cacheFile = new File("cacheTransaction" + File.separator + connectorId + File.separator + xid + ".data");
        if (cacheFile.exists()) {
            cacheFile.delete();
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

    public Map<String, List> getRedoLogContents() {
        if (isLarge()) {
            return chronicleMap;
        } else {
            return redoLogContents;
        }
    }

    public void setRedoLogContents(Map<String, List> redoLogContents) {
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
