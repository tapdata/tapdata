package io.tapdata.common.cdc;

import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.constant.SqlConstant;
import io.tapdata.constant.TapLog;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class LogMiner implements ILogMiner {

    protected final static int ROLLBACK_TEMP_LIMIT = 50; //max temp which can be rollback
    protected final static int LOG_QUEUE_SIZE = 5000; //size of queue which read logs

    private final static String TAG = LogMiner.class.getSimpleName();
    protected static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util
    public static final CCJBaseDDLWrapper.CCJDDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("\""); //DDL parser config
    protected DDLParserType ddlParserType; //DDL parser type
    protected final AtomicBoolean isRunning = new AtomicBoolean(false); //task running sign
    protected final AtomicBoolean ddlStop = new AtomicBoolean(false); //DDL task needs to stop DML flag
    protected ExecutorService redoLogConsumerThreadPool; //Log parsing thread

    protected final LinkedBlockingQueue<RedoLogContent> logQueue = new LinkedBlockingQueue<>(LOG_QUEUE_SIZE); //queue for logContent
    private int fullQueueWarn = 0;
    protected final LinkedHashMap<String, LogTransaction> transactionBucket = new LinkedHashMap<>(); //transaction cache
    protected RedoLogContent csfLogContent = null; //when redo or undo is too long, append them
    protected final Map<Long, Long> instanceThreadMindedSCNMap = new HashMap<>(); //Map<Thread#, SCN>
    protected final Map<Long, Long> instanceThreadSCNMap = new HashMap<>(); //Map<Thread#, SCN>
    protected boolean hasRollbackTemp; //whether rollback temp exists

    protected String connectorId;
    protected KVReadOnlyMap<TapTable> tableMap; //pdk tableMap in streamRead
    protected List<String> tableList; //tableName list
    protected Map<String, TapTable> lobTables; //table those have lob type
    protected int recordSize;
    protected StreamReadConsumer consumer;

    //init with pdk params
    @Override
    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        this.tableMap = tableMap;
        this.tableList = tableList;
        this.recordSize = recordSize;
        this.consumer = consumer;
        makeLobTables();
    }

    /**
     * collect tables which
     */
    protected void makeLobTables() {
        List<TapTable> lobTables = new ArrayList<>();
        tableList.forEach(table -> {
            TapTable tapTable = tableMap.get(table);
            if (tapTable.getNameFieldMap().entrySet().stream().anyMatch(field -> field.getValue().getDataType().contains("LOB"))) {
                lobTables.add(tapTable);
            }
        });
        this.lobTables = lobTables.stream().collect(Collectors.toMap(TapTable::getId, Function.identity()));
    }

    protected void enqueueRedoLogContent(RedoLogContent redoLogContent) {
        try {
            while (!logQueue.offer(redoLogContent, 1, TimeUnit.SECONDS)) {
                fullQueueWarn++;
                if (fullQueueWarn < 4) {
                    TapLogger.info(TAG, "log queue is full, waiting...");
                }
            }
            if (fullQueueWarn > 0) {
                TapLogger.info(TAG, "log queue has been released!");
                fullQueueWarn = 0;
            }
        } catch (InterruptedException ignore) {
        }
    }

    @Override
    public void stopMiner() throws Throwable {
        TapLogger.info(TAG, "Log Miner is shutting down...");
        isRunning.set(false);
        Optional.ofNullable(redoLogConsumerThreadPool).ifPresent(ExecutorService::shutdown);
        redoLogConsumerThreadPool = null;
    }

    protected void processOrBuffRedoLogContent(RedoLogContent redoLogContent,
                                               Consumer<Map<String, LogTransaction>> redoLogContentConsumer) {
        long scn = redoLogContent.getScn();
        String rsId = redoLogContent.getRsId();
        String xid = redoLogContent.getXid();
        String operation = redoLogContent.getOperation();

        if (hasRollbackTemp) {
            rollbackTempHandle(transactionBucket);
            final List<String> oracleTransactions = commitTempHandle(transactionBucket, redoLogContent);
            if (EmptyKit.isNotEmpty(oracleTransactions)) {
                for (String waitingCommitXid : oracleTransactions) {
                    final LogTransaction logTransaction = transactionBucket.get(waitingCommitXid);
                    if (logTransaction != null) {
                        TapLogger.info(TAG, "Delay commit transaction[scn: {}, xid: {}], redo size: {}",
                                logTransaction.getScn(), logTransaction.getXid(), logTransaction.getSize());
                        commitTransaction(redoLogContentConsumer, logTransaction);
                    }
                }
            }
        }

        switch (operation) {
            case SqlConstant.REDO_LOG_OPERATION_INSERT:
            case SqlConstant.REDO_LOG_OPERATION_UPDATE:
            case SqlConstant.REDO_LOG_OPERATION_DELETE:
            case SqlConstant.REDO_LOG_OPERATION_SELECT_FOR_UPDATE:
            case SqlConstant.REDO_LOG_OPERATION_LOB_TRIM:
            case SqlConstant.REDO_LOG_OPERATION_LOB_WRITE:
            case SqlConstant.REDO_LOG_OPERATION_SEL_LOB_LOCATOR:
                if (!transactionBucket.containsKey(xid)) {
                    TapLogger.debug(TAG, TapLog.D_CONN_LOG_0003.getMsg(), xid);
                    Map<String, List> redoLogContents = new LinkedHashMap<>();
                    redoLogContents.put(rsId, new ArrayList<>(4));
                    redoLogContents.get(rsId).add(redoLogContent);
                    LogTransaction orclTransaction = new LogTransaction(rsId, scn, xid, redoLogContents, redoLogContent.getTimestamp().getTime());
                    orclTransaction.setConnectorId(connectorId);
                    setRacMinimalScn(orclTransaction);
                    orclTransaction.incrementSize(1);
                    transactionBucket.put(xid, orclTransaction);
                } else {
                    LogTransaction logTransaction = transactionBucket.get(xid);
                    Map<String, List> redoLogContents = logTransaction.getRedoLogContents();
                    try {
                        if (!needToAborted(operation, redoLogContent, redoLogContents)) {
                            logTransaction.addRedoLogContent(redoLogContent);
                            logTransaction.incrementSize(1);
                            long txLogContentsSize = logTransaction.getSize();
                            if (txLogContentsSize % LogTransaction.LARGE_TRANSACTION_UPPER_LIMIT == 0) {
                                TapLogger.info(TAG, TapLog.CON_LOG_0008.getMsg(), xid, txLogContentsSize);
                            }
                        }
                    } catch (Exception e) {
                        TapLogger.error(TAG, e.getMessage());
                    }
                }
                break;
            case SqlConstant.REDO_LOG_OPERATION_COMMIT:
                if (transactionBucket.containsKey(xid)) {
                    LogTransaction orclTransaction = transactionBucket.get(xid);
                    if (!need2WaitingCommit(orclTransaction)) {
                        commitTransaction(redoLogContentConsumer, orclTransaction);
                    } else {
                        orclTransaction.setRollbackTemp(0);
                        orclTransaction.setLastTimestamp(redoLogContent.getTimestamp().getTime());
                        orclTransaction.setLastCommitTimestamp(redoLogContent.getCommitTimestamp().getTime());
                    }

                } else {
                    Map<String, List> redoLogContents = new LinkedHashMap<>();
                    redoLogContents.put(rsId, new ArrayList<>(4));
                    redoLogContents.get(rsId).add(redoLogContent);
                    LogTransaction orclTransaction = new LogTransaction(rsId, scn, xid, redoLogContents);
                    orclTransaction.setConnectorId(connectorId);
                    setRacMinimalScn(orclTransaction);
                    orclTransaction.setTransactionType(LogTransaction.TX_TYPE_COMMIT);
                    orclTransaction.incrementSize(1);
                    Map<String, LogTransaction> cacheCommitTraction = new HashMap<>();
                    cacheCommitTraction.put(xid, orclTransaction);
                    redoLogContentConsumer.accept(cacheCommitTraction);
                }
                break;
            case SqlConstant.REDO_LOG_OPERATION_DDL:
                TapLogger.debug(TAG, TapLog.D_CONN_LOG_0003.getMsg(), xid);
                Map<String, List> redoLogContents = new LinkedHashMap<>();
                redoLogContents.put(rsId, new ArrayList<>(4));
                redoLogContents.get(rsId).add(redoLogContent);
                LogTransaction orclTransaction = new LogTransaction(rsId, scn, xid, redoLogContents);
                orclTransaction.setConnectorId(connectorId);
                setRacMinimalScn(orclTransaction);
                orclTransaction.setTransactionType(LogTransaction.TX_TYPE_DDL);
                Map<String, LogTransaction> cacheCommitTraction = new HashMap<>();
                cacheCommitTraction.put(xid, orclTransaction);
                redoLogContentConsumer.accept(cacheCommitTraction);
                break;
            case SqlConstant.REDO_LOG_OPERATION_ROLLBACK:
                if (transactionBucket.containsKey(xid)) {
                    LogTransaction logTransaction = transactionBucket.get(xid);
                    if (logTransaction.isLarge()) {
                        TapLogger.info(TAG, "Found large transaction be rolled back: {}", logTransaction);
                    }
                    hasRollbackTemp = true;
                    logTransaction.setRollbackTemp(1);
                    logTransaction.setHasRollback(true);
                }
                break;
            default:
                break;
        }
    }

    protected void sendTransaction(Map<String, LogTransaction> txMap) {
        for (Map.Entry<String, LogTransaction> txEntry : txMap.entrySet()) {
            LogTransaction logTransaction = txEntry.getValue();
            AtomicReference<List<TapEvent>> eventList = new AtomicReference<>(TapSimplify.list());
            AtomicReference<RedoLogContent> lastRedoLogContent = new AtomicReference<>();
            Map<String, List> redoLogContents = logTransaction.getRedoLogContents();
            if (logTransaction.isLarge()) {
                String keyTemp;
                while ((keyTemp = logTransaction.pollKey()) != null) {
                    batchCreateEvents(redoLogContents.get(keyTemp), eventList, lastRedoLogContent);
                    if (eventList.get().size() >= 1000) {
                        submitEvent(lastRedoLogContent.get(), eventList.get());
                        eventList.set(TapSimplify.list());
                    }
                }
                submitEvent(lastRedoLogContent.get(), eventList.get());
            } else {
                for (List<RedoLogContent> redoLogContentList : redoLogContents.values()) {
                    batchCreateEvents(redoLogContentList, eventList, lastRedoLogContent);
                }
                submitEvent(lastRedoLogContent.get(), eventList.get());
            }
            txEntry.getValue().clearRedoLogContents();
        }
    }

    private void batchCreateEvents(List<RedoLogContent> redoLogContentList, AtomicReference<List<TapEvent>> eventList, AtomicReference<RedoLogContent> lastRedoLogContent) {
        for (RedoLogContent redoLogContent : redoLogContentList) {
            lastRedoLogContent.set(redoLogContent);
            if (EmptyKit.isNull(Objects.requireNonNull(redoLogContent).getRedoRecord()) && !"DDL".equals(Objects.requireNonNull(redoLogContent).getOperation())) {
                continue;
            }
            switch (Objects.requireNonNull(redoLogContent).getOperation()) {
                case "INSERT":
                    eventList.get().add(new TapInsertRecordEvent().init()
                            .table(redoLogContent.getTableName())
                            .after(redoLogContent.getRedoRecord())
                            .referenceTime(redoLogContent.getTimestamp().getTime()));
                    break;
                case "UPDATE":
                    eventList.get().add(new TapUpdateRecordEvent().init()
                            .table(redoLogContent.getTableName())
                            .after(redoLogContent.getRedoRecord())
                            .before(redoLogContent.getUndoRecord())
                            .referenceTime(redoLogContent.getTimestamp().getTime()));
                    break;
                case "DELETE":
                    eventList.get().add(new TapDeleteRecordEvent().init()
                            .table(redoLogContent.getTableName())
                            .before(redoLogContent.getRedoRecord())
                            .referenceTime(redoLogContent.getTimestamp().getTime()));
                    break;
                case "DDL":
                    try {
                        ddlStop.set(true);
                        TapSimplify.sleep(5000);
                        ddlFlush();
                        ddlStop.set(false);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        long referenceTime = redoLogContent.getTimestamp().getTime();
                        TapLogger.warn(TAG, "DDL [{}] is synchronizing...", redoLogContent.getSqlRedo());
                        DDLFactory.ddlToTapDDLEvent(ddlParserType, redoLogContent.getSqlRedo(),
                                DDL_WRAPPER_CONFIG,
                                tableMap,
                                tapDDLEvent -> {
                                    tapDDLEvent.setTime(System.currentTimeMillis());
                                    tapDDLEvent.setReferenceTime(referenceTime);
                                    eventList.get().add(tapDDLEvent);
                                });
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    break;
            }
        }
    }

    protected abstract void ddlFlush() throws Throwable;

    protected abstract void submitEvent(RedoLogContent redoLogContent, List<TapEvent> list);

    private void rollbackTempHandle(LinkedHashMap<String, LogTransaction> transactionBucket) {
        if (EmptyKit.isEmpty(transactionBucket)) {
            return;
        }
        Iterator<String> iterator = transactionBucket.keySet().iterator();
        hasRollbackTemp = false;
        while (iterator.hasNext()) {
            String bucketXid = iterator.next();
            LogTransaction bucketTransaction = transactionBucket.get(bucketXid);
            int rollbackTemp = bucketTransaction.getRollbackTemp();
            if (bucketTransaction.isHasRollback()) {
                hasRollbackTemp = true;
            }
            if (rollbackTemp <= 0) {
                continue;
            }
            if (rollbackTemp < ROLLBACK_TEMP_LIMIT) {
                bucketTransaction.setRollbackTemp(++rollbackTemp);
                hasRollbackTemp = true;
            } else {
                TapLogger.info(TAG, "It was found that the transaction[first scn: {}, xid: {}] that was rolled back did not commit after {} events, " +
                        "and the modification of this transaction was truly discarded", bucketTransaction.getScn(), bucketXid, ROLLBACK_TEMP_LIMIT);
                iterator.remove();
            }
        }
    }

    private List<String> commitTempHandle(LinkedHashMap<String, LogTransaction> transactionBucket, RedoLogContent redoLogContent) {
        List<String> need2CommitTxs = new ArrayList<>();
        if (EmptyKit.isNotEmpty(transactionBucket)) {
            transactionBucket.values().forEach(logTransaction -> {
                if (logTransaction == null || logTransaction.getLastTimestamp() == null || logTransaction.getLastCommitTimestamp() == null
                        || redoLogContent.getTimestamp() == null || redoLogContent.getCommitTimestamp() == null) {
                    return;
                }
                if (logTransaction.getLastTimestamp().compareTo(redoLogContent.getTimestamp().getTime()) < 0 ||
                        logTransaction.getLastCommitTimestamp().compareTo(redoLogContent.getCommitTimestamp().getTime()) < 0) {
                    need2CommitTxs.add(logTransaction.getXid());
                }
            });
        }
        return need2CommitTxs;
    }

    private void commitTransaction(Consumer<Map<String, LogTransaction>> redoLogContentConsumer, LogTransaction orclTransaction) {
        final String xid = orclTransaction.getXid();
        transactionBucket.remove(xid);
        long txLogContentsSize = orclTransaction.getSize();
        if (orclTransaction.isHasRollback()) {
            TapLogger.info(TAG, "Found commit that had a rollback before it, first scn: {}, xid: {}, log content size: {}", orclTransaction.getScn(), xid, txLogContentsSize);
        }
        if (txLogContentsSize >= LogTransaction.LARGE_TRANSACTION_UPPER_LIMIT) {
            TapLogger.info(TAG, TapLog.D_CONN_LOG_0002.getMsg(), xid, txLogContentsSize);
        } else {
            TapLogger.debug(TAG, TapLog.D_CONN_LOG_0002.getMsg(), xid, txLogContentsSize);
        }
        Map<String, LogTransaction> cacheCommitTraction = new HashMap<>();
        cacheCommitTraction.put(xid, orclTransaction);
        redoLogContentConsumer.accept(cacheCommitTraction);
    }

    private void setRacMinimalScn(LogTransaction logTransaction) {
        if (EmptyKit.isNotEmpty(instanceThreadMindedSCNMap) && instanceThreadMindedSCNMap.size() > 1) {
            long racMinimalSCN = 0L;
            for (Long mindedSCN : instanceThreadMindedSCNMap.values()) {
                racMinimalSCN = racMinimalSCN < mindedSCN ? racMinimalSCN : mindedSCN;
            }
            logTransaction.setRacMinimalScn(racMinimalSCN);
        }
    }

    private boolean needToAborted(String operation, RedoLogContent redoLogContent, Map<String, List> redoLogContents) {
        boolean needToAborted = false;
        if (EmptyKit.isNotBlank(redoLogContent.getSqlUndo()) || EmptyKit.isNotEmpty(redoLogContent.getRedoRecord())) {
            return false;
        }
        String rowId = redoLogContent.getRowId();
        if (SqlConstant.REDO_LOG_OPERATION_DELETE.equals(operation)) {
            Iterator<String> keySetIter = redoLogContents.keySet().iterator();
            while (keySetIter.hasNext()) {
                String key = keySetIter.next();
                List<RedoLogContent> logContents = redoLogContents.get(key);
                Iterator<RedoLogContent> iterator = logContents.iterator();
                while (iterator.hasNext()) {
                    RedoLogContent logContent = iterator.next();
                    if (SqlConstant.REDO_LOG_OPERATION_INSERT.equals(logContent.getOperation())) {
                        String insertedRowId = logContent.getRowId();
                        if (insertedRowId.equals(rowId)) {
                            TapLogger.info("Found insert row was deleted by row id {} on the same transaction, insert event {}, delete event {}", rowId, logContent, redoLogContent);
                            iterator.remove();
                            needToAborted = true;
                        }
                    }
                }
                if (needToAborted && EmptyKit.isEmpty(logContents)) {
                    keySetIter.remove();
                }
            }
        } else if (SqlConstant.REDO_LOG_OPERATION_UPDATE.equals(operation)) {
            try {
                String currentBetweenSetAndWhere = StringKit.subStringBetweenTwoString(redoLogContent.getSqlRedo(), "set", "where");
                if (EmptyKit.isBlank(currentBetweenSetAndWhere) && EmptyKit.isEmpty(redoLogContent.getRedoRecord())) {
                    return true;
                }
                Iterator<String> keyIter = redoLogContents.keySet().iterator();
                while (keyIter.hasNext() && !needToAborted) {
                    List<RedoLogContent> logContents = redoLogContents.get(keyIter.next());
                    Iterator<RedoLogContent> iterator = logContents.iterator();
                    while (iterator.hasNext()) {
                        RedoLogContent logContent = iterator.next();
                        if (!SqlConstant.REDO_LOG_OPERATION_UPDATE.equals(logContent.getOperation()) || !rowId.equals(logContent.getRowId())) {
                            continue;
                        }
                        String betweenSetAndWhere = StringKit.subStringBetweenTwoString(logContent.getSqlUndo(), "set", "where");
                        if (currentBetweenSetAndWhere.equals(betweenSetAndWhere)) {
                            needToAborted = true;
                        } else if (redoLogContent.getRollback() == 1 && StringKit.indexOf(betweenSetAndWhere, currentBetweenSetAndWhere.trim()) > -1) {
                            needToAborted = true;
                        }
                        if (needToAborted) {
                            TapLogger.debug(TAG, "Found update row was undo updated by row id {} on the same transaction, update event {}, undo update event {}", rowId, logContent, redoLogContent);
                            iterator.remove();
                            break;
                        }
                    }
                    if (needToAborted && EmptyKit.isEmpty(logContents)) {
                        keyIter.remove();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(String.format("Check abort update oracle log failed, err: %s, scn: %s, xid: %s, timestamp: %s",
                        e.getMessage(), redoLogContent.getScn(), redoLogContent.getXid(), redoLogContent.getTimestamp()), e);
            }
        } else if (SqlConstant.REDO_LOG_OPERATION_INSERT.equals(operation)) {
            if (EmptyKit.isBlank(redoLogContent.getSqlRedo()) && EmptyKit.isEmpty(redoLogContent.getRedoRecord())) {
                return true;
            }
            Iterator<String> keyIter = redoLogContents.keySet().iterator();
            while (keyIter.hasNext()) {
                List<RedoLogContent> logContents = redoLogContents.get(keyIter.next());
                Iterator<RedoLogContent> iterator = logContents.iterator();
                while (iterator.hasNext()) {
                    RedoLogContent logContent = iterator.next();
                    if (!SqlConstant.REDO_LOG_OPERATION_DELETE.equals(logContent.getOperation())) {
                        continue;
                    }
                    if (EmptyKit.isBlank(logContent.getSqlRedo())) {
                        continue;
                    }
                    if (rowId.equals(logContent.getRowId())
                            && redoLogContent.getSqlRedo().equals(logContent.getSqlUndo())
                    ) {
                        TapLogger.info(TAG, "Found delete row was undo inserted by row id {} on the same transaction, delete event {}, undo insert event {}", rowId, logContent, redoLogContent);
                        iterator.remove();
                        needToAborted = true;
                    }
                }
                if (needToAborted && EmptyKit.isEmpty(logContents)) {
                    keyIter.remove();
                }
            }
        }
        return needToAborted;
    }

    private boolean need2WaitingCommit(LogTransaction transaction) {
        transaction.setReceivedCommitTs(System.currentTimeMillis());
        return transaction.isHasRollback();
    }
}
