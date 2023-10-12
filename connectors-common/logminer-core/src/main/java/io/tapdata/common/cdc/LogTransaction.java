package io.tapdata.common.cdc;

import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.kit.StringKit;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.ValueOut;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by tapdata on 23/03/2018.
 */
public class LogTransaction {

    public static final String TX_TYPE_DDL = "ddl";
    public static final String TX_TYPE_DML = "dml";
    public static final String TX_TYPE_COMMIT = "commit";
    private long largeTransactionUpperLimit = 10000L;

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
    private ChronicleQueue keyQueue;
    private ExcerptAppender keyQueueAppender;
    private ExcerptTailer keyTailer;

    //局部回滚
    private Map<String, Long> partRollbackMap = new HashMap<>();

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

    public void setLargeTransactionUpperLimit(long largeTransactionUpperLimit) {
        this.largeTransactionUpperLimit = largeTransactionUpperLimit;
    }

    public long getLargeTransactionUpperLimit() {
        return largeTransactionUpperLimit;
    }

    public void addRedoLogContent(RedoLogContent redoLogContent) throws IOException {
        if (redoLogContent.getRollback() == 1 && EmptyKit.isBlank(redoLogContent.getSqlUndo())) {
            String rollbackKey = redoLogContent.getRowId();
            switch (redoLogContent.getOperation()) {
                case "INSERT":
                    rollbackKey += "|" + "DELETE" + "|" + redoLogContent.getSqlRedo();
                    break;
                case "UPDATE":
                    rollbackKey += "|" + "UPDATE" + "|" + StringKit.subStringBetweenTwoString(redoLogContent.getSqlRedo(), "set", "where");
                    break;
                case "DELETE":
                    rollbackKey += "|" + "INSERT";
                    break;
            }
            if (!partRollbackMap.containsKey(rollbackKey)) {
                partRollbackMap.put(rollbackKey, 1L);
            } else {
                partRollbackMap.put(rollbackKey, partRollbackMap.get(rollbackKey) + 1);
            }
        } else {
            String rsId = redoLogContent.getRsId();
            if (EmptyKit.isNull(redoLogContents)) {
                redoLogContents = new LinkedHashMap<>();
            }
            if (size >= largeTransactionUpperLimit) {
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
                    keyQueue = SingleChronicleQueueBuilder.binary("cacheTransaction" + File.separator + connectorId + File.separator + xid + ".key").build();
                    keyQueueAppender = keyQueue.acquireAppender();
                    keyTailer = keyQueue.createTailer();
                    chronicleMap.putAll(redoLogContents);
                    redoLogContents.forEach((k, v) -> pushKey(k));
                }
                if (!chronicleMap.containsKey(rsId)) {
                    List<RedoLogContent> list = new ArrayList<>();
                    list.add(redoLogContent);
                    chronicleMap.put(rsId, list);
                    pushKey(rsId);
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
            size++;
        }
    }

    public void decreasePartRollback(String key) {
        if (partRollbackMap.containsKey(key)) {
            long count = partRollbackMap.get(key) - 1;
            if (count > 0) {
                partRollbackMap.put(key, count);
            } else {
                partRollbackMap.remove(key);
            }
        }
    }

    private void pushKey(String key) {
        keyQueueAppender.writeDocument(w -> {
            final ValueOut valueOut = w.getValueOut();
            valueOut.writeString(key);
        });
    }

    public String pollKey() {
        AtomicReference<String> key = new AtomicReference<>();
        if (keyTailer.readDocument(r -> key.set(r.getValueIn().readString()))) {
            return key.get();
        } else {
            return null;
        }
    }

    public void clearRedoLogContents() {
        if (EmptyKit.isNotEmpty(redoLogContents)) {
            redoLogContents.clear();
        }
        if (isLarge()) {
            if (EmptyKit.isNotEmpty(chronicleMap)) {
                chronicleMap.clear();
                chronicleMap.close();
            }
            if (EmptyKit.isNotNull(keyQueueAppender)) {
                keyQueueAppender.close();
            }
            if (EmptyKit.isNotNull(keyTailer)) {
                keyTailer.close();
            }
            if (EmptyKit.isNotNull(keyQueue)) {
                keyQueue.close();
            }
            ErrorKit.ignoreAnyError(() -> FileSystemUtils.deleteRecursively(new File("cacheTransaction" + File.separator + connectorId + File.separator + xid + ".data")));
            ErrorKit.ignoreAnyError(() -> FileSystemUtils.deleteRecursively(new File("cacheTransaction" + File.separator + connectorId + File.separator + xid + ".key")));
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
        return this.size > largeTransactionUpperLimit;
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

    public Map<String, Long> getPartRollbackMap() {
        return partRollbackMap;
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
