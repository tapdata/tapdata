package io.tapdata.common.cdc;

import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.ValueOut;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

public class NormalTransaction {

    private long largeTransactionUpperLimit = 10000L;
    private String connectorId;
    private Long cdcSequenceId;
    private String cdcSequenceStr;
    private String transactionId;
    private LinkedList<NormalRedo> normalRedoes;
    private ChronicleQueue chronicleQueue;
    private ExcerptAppender redoQueueAppender;
    private ExcerptTailer redoTailor;
    private long size;
    private Long minCdcSequenceId;
    private Long firstTimestamp;
    private Long lastTimestamp;

    public NormalTransaction(String cdcSequenceStr, String transactionId, LinkedList<NormalRedo> normalRedoes) {
        this.cdcSequenceStr = cdcSequenceStr;
        this.transactionId = transactionId;
        this.normalRedoes = normalRedoes;
    }

    public NormalTransaction(Long cdcSequenceId, String transactionId, LinkedList<NormalRedo> normalRedoes) {
        this.cdcSequenceId = cdcSequenceId;
        this.transactionId = transactionId;
        this.normalRedoes = normalRedoes;
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

    public void pushRedo(NormalRedo normalRedo) {
        if (EmptyKit.isNull(normalRedoes)) {
            normalRedoes = new LinkedList<>();
        }
        if (size >= largeTransactionUpperLimit) {
            if (EmptyKit.isNull(chronicleQueue)) {
                File cacheDir = new File("cacheTransaction" + File.separator + connectorId);
                if (!cacheDir.exists()) {
                    cacheDir.mkdirs();
                }
                chronicleQueue = SingleChronicleQueueBuilder.binary("cacheTransaction" + File.separator + connectorId + File.separator + transactionId + ".data").build();
                redoQueueAppender = chronicleQueue.acquireAppender();
                redoTailor = chronicleQueue.createTailer();
                redoQueueAppender.writeDocument(w -> {
                    final ValueOut valueOut = w.getValueOut();
                    normalRedoes.forEach(this::pushChronicleRedo);
                    valueOut.list(normalRedoes, NormalRedo.class);
                });
            }
            pushChronicleRedo(normalRedo);
        } else {
            normalRedoes.add(normalRedo);
        }
        size++;
    }

    public NormalRedo pollRedo() {
        if (isLarge()) {
            return pollChronicleRedo();
        } else {
            return normalRedoes.poll();
        }
    }

    private void pushChronicleRedo(NormalRedo redo) {
        redoQueueAppender.writeDocument(redo);
    }

    private NormalRedo pollChronicleRedo() {
        NormalRedo redo = new NormalRedo();
        redoTailor.readDocument(redo);
        return redo;
    }

    public void clearRedoes() {
        if (EmptyKit.isNotEmpty(normalRedoes)) {
            normalRedoes.clear();
        }
        if (isLarge()) {
            if (EmptyKit.isNotNull(redoQueueAppender)) {
                redoQueueAppender.close();
            }
            if (EmptyKit.isNotNull(redoTailor)) {
                redoTailor.close();
            }
            if (EmptyKit.isNotNull(chronicleQueue)) {
                chronicleQueue.close();
            }
            ErrorKit.ignoreAnyError(() -> FileSystemUtils.deleteRecursively(new File("cacheTransaction" + File.separator + connectorId + File.separator + transactionId + ".data")));
        }
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

    public Long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(Long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    public String getConnectorId() {
        return connectorId;
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

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public List<NormalRedo> getNormalRedoes() {
        return normalRedoes;
    }

    public void setNormalRedoes(LinkedList<NormalRedo> normalRedoes) {
        this.normalRedoes = normalRedoes;
    }

    public Long getMinCdcSequenceId() {
        return minCdcSequenceId;
    }

    public void setMinCdcSequenceId(Long minCdcSequenceId) {
        this.minCdcSequenceId = minCdcSequenceId;
    }

    public void setFirstTimestamp(Long firstTimestamp) {
        this.firstTimestamp = firstTimestamp;
    }
}
