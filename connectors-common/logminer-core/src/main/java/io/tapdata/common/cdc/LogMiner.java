package io.tapdata.common.cdc;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.constant.SqlConstant;
import io.tapdata.constant.TapLog;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;

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

    protected static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util
    public static final CCJBaseDDLWrapper.CCJDDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("\""); //DDL parser config
    protected DDLParserType ddlParserType; //DDL parser type
    protected final AtomicBoolean isRunning = new AtomicBoolean(false); //task running sign
    protected final AtomicBoolean ddlStop = new AtomicBoolean(false); //DDL task needs to stop DML flag
    protected ExecutorService redoLogConsumerThreadPool; //Log parsing thread

    protected final LinkedBlockingQueue<RedoLogContent> logQueue = new LinkedBlockingQueue<>(LOG_QUEUE_SIZE); //queue for logContent
    private int fullQueueWarn = 0;
    private long largeTransactionUpperLimit = 10000L;
    protected final ConcurrentLinkedHashMap<String, LogTransaction> transactionBucket = new ConcurrentLinkedHashMap.Builder<String, LogTransaction>()
            .initialCapacity(16).maximumWeightedCapacity(1024000).build();
    ; //transaction cache
    protected RedoLogContent csfLogContent = null; //when redo or undo is too long, append them
    protected final Map<Long, Long> instanceThreadMindedSCNMap = new HashMap<>(); //Map<Thread#, SCN>
    protected final Map<Long, Long> instanceThreadSCNMap = new HashMap<>(); //Map<Thread#, SCN>
    protected boolean hasRollbackTemp; //whether rollback temp exists

    protected String connectorId;
    protected Log tapLogger;
    protected KVReadOnlyMap<TapTable> tableMap; //pdk tableMap in streamRead
    protected List<String> tableList; //tableName list
    protected Map<String, List<String>> schemaTableMap; //schemaName and tableName map
    protected Map<String, TapTable> lobTables; //table those have lob type
    protected int recordSize;
    protected StreamReadConsumer consumer;
    protected AtomicReference<Throwable> threadException = new AtomicReference<>();
    protected Boolean withSchema = false;

    //init with pdk params
    @Override
    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        this.tableMap = tableMap;
        this.tableList = tableList;
        this.recordSize = recordSize;
        this.consumer = consumer;
        makeLobTables();
    }

    //multi init with pdk params
    @Override
    public void multiInit(List<ConnectionConfigWithTables> connectionConfigWithTables, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        this.withSchema = true;
        this.tableMap = tableMap;
        this.schemaTableMap = new HashMap<>();
		for (ConnectionConfigWithTables withTables : connectionConfigWithTables) {
			if (null == withTables.getConnectionConfig())
				throw new RuntimeException("Not found connection config");
			if (null == withTables.getConnectionConfig().get("schema"))
				throw new RuntimeException("Not found connection schema");
			if (null == withTables.getTables())
				throw new RuntimeException("Not found connection tables");

			schemaTableMap.compute(String.valueOf(withTables.getConnectionConfig().get("schema")), (schema, tableList) -> {
				if (null == tableList) {
					tableList = new ArrayList<>();
				}

				for (String tableName : withTables.getTables()) {
					if (!tableList.contains(tableName)) {
						tableList.add(tableName);
					}
				}
				return tableList;
			});
		}
        this.recordSize = recordSize;
        this.consumer = consumer;
        DDL_WRAPPER_CONFIG.withSchema(true);
        multiMakeLobTables();
    }

    public void setLargeTransactionUpperLimit(long largeTransactionUpperLimit) {
        this.largeTransactionUpperLimit = largeTransactionUpperLimit;
    }

    /**
     * collect tables which
     */
    protected void makeLobTables() {
        List<TapTable> lobTables = new ArrayList<>();
        tableList.forEach(table -> {
            TapTable tapTable = tableMap.get(table);
            if (null == tapTable || null == tapTable.getNameFieldMap()) {
                return;
            }
            if (tapTable.getNameFieldMap().entrySet().stream().anyMatch(field -> field.getValue().getDataType().contains("LOB"))) {
                lobTables.add(tapTable);
            }
        });
        this.lobTables = lobTables.stream().collect(Collectors.toMap(TapTable::getId, Function.identity()));
    }

    //multi makeLobTables
    protected void multiMakeLobTables() {
        lobTables = new HashMap<>();
        schemaTableMap.forEach((schema, tables) -> tables.forEach(table -> {
            TapTable tapTable = tableMap.get(schema + "." + table);
            if (null == tapTable || null == tapTable.getNameFieldMap()) {
                return;
            }
            if (tapTable.getNameFieldMap().entrySet().stream().anyMatch(field -> field.getValue().getDataType().contains("LOB"))) {
                lobTables.put(schema + "." + table, tapTable);
            }
        }));
    }

    protected void enqueueRedoLogContent(RedoLogContent redoLogContent) {
        try {
            while (!logQueue.offer(redoLogContent, 1, TimeUnit.SECONDS)) {
                fullQueueWarn++;
                if (fullQueueWarn < 4) {
                    tapLogger.info("log queue is full, waiting...");
                }
            }
            if (fullQueueWarn > 0) {
                tapLogger.info("log queue has been released!");
                fullQueueWarn = 0;
            }
        } catch (InterruptedException ignore) {
        }
    }

    @Override
    public void stopMiner() throws Throwable {
        tapLogger.info("Log Miner is shutting down...");
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
                        tapLogger.info("Delay commit transaction[scn: {}, xid: {}], redo size: {}",
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
                    tapLogger.debug(TapLog.D_CONN_LOG_0003.getMsg(), xid);
                    Map<String, List> redoLogContents = new LinkedHashMap<>();
                    redoLogContents.put(rsId, new ArrayList<>(4));
                    redoLogContents.get(rsId).add(redoLogContent);
                    LogTransaction orclTransaction = new LogTransaction(rsId, scn, xid, redoLogContents, redoLogContent.getTimestamp().getTime());
                    orclTransaction.setLargeTransactionUpperLimit(largeTransactionUpperLimit);
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
                            if (txLogContentsSize % logTransaction.getLargeTransactionUpperLimit() == 0) {
                                tapLogger.info(TapLog.CON_LOG_0008.getMsg(), xid, txLogContentsSize);
                            }
                        }
                    } catch (Exception e) {
                        tapLogger.error("Error redoLogContent:{}", redoLogContent.toString());
                        tapLogger.error(e.getMessage());
                        throw new RuntimeException(e);
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
                    orclTransaction.setLargeTransactionUpperLimit(largeTransactionUpperLimit);
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
                tapLogger.debug(TapLog.D_CONN_LOG_0003.getMsg(), xid);
                Map<String, List> redoLogContents = new LinkedHashMap<>();
                redoLogContents.put(rsId, new ArrayList<>(4));
                redoLogContents.get(rsId).add(redoLogContent);
                LogTransaction orclTransaction = new LogTransaction(rsId, scn, xid, redoLogContents);
                orclTransaction.setLargeTransactionUpperLimit(largeTransactionUpperLimit);
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
                        tapLogger.debug("Found large transaction be rolled back: {}", logTransaction);
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
                    batchCreateEvents(redoLogContents.get(keyTemp), eventList, lastRedoLogContent, logTransaction.getReceivedCommitTs());
                    if (eventList.get().size() >= 1000) {
                        submitEvent(lastRedoLogContent.get(), eventList.get());
                        eventList.set(TapSimplify.list());
                    }
                }
                submitEvent(lastRedoLogContent.get(), eventList.get());
            } else {
                for (List<RedoLogContent> redoLogContentList : redoLogContents.values()) {
                    batchCreateEvents(redoLogContentList, eventList, lastRedoLogContent, logTransaction.getReceivedCommitTs());
                }
                submitEvent(lastRedoLogContent.get(), eventList.get());
            }
            txEntry.getValue().clearRedoLogContents();
        }
    }

    protected abstract void ddlFlush() throws Throwable;

    protected abstract void batchCreateEvents(List<RedoLogContent> redoLogContentList, AtomicReference<List<TapEvent>> eventList, AtomicReference<RedoLogContent> lastRedoLogContent, long timestamp);

    protected abstract void submitEvent(RedoLogContent redoLogContent, List<TapEvent> list);

    private void rollbackTempHandle(ConcurrentLinkedHashMap<String, LogTransaction> transactionBucket) {
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
                tapLogger.info("It was found that the transaction[first scn: {}, xid: {}] that was rolled back did not commit after {} events, " +
                        "and the modification of this transaction was truly discarded", bucketTransaction.getScn(), bucketXid, ROLLBACK_TEMP_LIMIT);
                bucketTransaction.clearRedoLogContents();
                iterator.remove();
            }
        }
    }

    private List<String> commitTempHandle(ConcurrentLinkedHashMap<String, LogTransaction> transactionBucket, RedoLogContent redoLogContent) {
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
            tapLogger.info("Found commit that had a rollback before it, first scn: {}, xid: {}, log content size: {}", orclTransaction.getScn(), xid, txLogContentsSize);
        }
        if (txLogContentsSize >= orclTransaction.getLargeTransactionUpperLimit()) {
            tapLogger.info(TapLog.D_CONN_LOG_0002.getMsg(), xid, txLogContentsSize);
        } else {
            tapLogger.debug(TapLog.D_CONN_LOG_0002.getMsg(), xid, txLogContentsSize);
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
                            tapLogger.info("Found insert row was deleted by row id {} on the same transaction, insert event {}, delete event {}", rowId, logContent, redoLogContent);
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
                            tapLogger.debug("Found update row was undo updated by row id {} on the same transaction, update event {}, undo update event {}", rowId, logContent, redoLogContent);
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
                        tapLogger.info("Found delete row was undo inserted by row id {} on the same transaction, delete event {}, undo insert event {}", rowId, logContent, redoLogContent);
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
