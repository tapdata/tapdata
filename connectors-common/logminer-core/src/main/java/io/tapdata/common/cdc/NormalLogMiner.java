package io.tapdata.common.cdc;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.constant.TapLog;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
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

public abstract class NormalLogMiner implements ILogMiner {
    protected final static int LOG_QUEUE_SIZE = 5000; //size of queue which read logs
    protected static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util
    public static final CCJBaseDDLWrapper.CCJDDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("\""); //DDL parser config
    protected DDLParserType ddlParserType; //DDL parser type
    protected final AtomicBoolean isRunning = new AtomicBoolean(false); //task running sign
    protected final AtomicBoolean ddlStop = new AtomicBoolean(false); //DDL task needs to stop DML flag
    protected ExecutorService redoLogConsumerThreadPool; //Log parsing thread

    protected final LinkedBlockingQueue<NormalRedo> logQueue = new LinkedBlockingQueue<>(LOG_QUEUE_SIZE); //queue for logContent
    private int fullQueueWarn = 0;
    private long largeTransactionUpperLimit = 10000L;
    protected final ConcurrentLinkedHashMap<String, NormalTransaction> transactionBucket = new ConcurrentLinkedHashMap.Builder<String, NormalTransaction>()
            .initialCapacity(16).maximumWeightedCapacity(1024000).build();

    protected String connectorId;
    protected Log tapLogger;
    protected KVReadOnlyMap<TapTable> tableMap; //pdk tableMap in streamRead
    protected List<String> tableList; //tableName list
    protected Map<String, TapTable> lobTables; //table those have lob type
    protected int recordSize;
    protected StreamReadConsumer consumer;
    protected AtomicReference<Throwable> threadException = new AtomicReference<>();

    //init with pdk params
    @Override
    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        this.tableMap = tableMap;
        this.tableList = tableList;
        this.recordSize = recordSize;
        this.consumer = consumer;
        makeLobTables();
    }

    public void setLargeTransactionUpperLimit(long largeTransactionUpperLimit) {
        this.largeTransactionUpperLimit = largeTransactionUpperLimit;
    }

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

    protected void enqueueRedoLogContent(NormalRedo normalRedo) {
        try {
            while (!logQueue.offer(normalRedo, 1, TimeUnit.SECONDS)) {
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
        transactionBucket.values().forEach(logTransaction -> {
            if (logTransaction.isLarge()) {
                logTransaction.clearRedoes();
            }
        });
    }

    protected void processOrBuffRedo(NormalRedo normalRedo,
                                     Consumer<Map<String, NormalTransaction>> redoConsumer) {
        Long cdcSequenceId = normalRedo.getCdcSequenceId();
        String transactionId = normalRedo.getTransactionId();
        String operation = normalRedo.getOperation();

        switch (operation) {
            case "BEGIN": {
                NormalTransaction transaction = new NormalTransaction(cdcSequenceId, transactionId, new LinkedList<>());
                transaction.setFirstTimestamp(normalRedo.getTimestamp());
                transaction.setLargeTransactionUpperLimit(largeTransactionUpperLimit);
                transaction.setConnectorId(connectorId);
                transactionBucket.put(transactionId, transaction);
                break;
            }
            case "INSERT":
            case "UPDATE":
            case "DELETE": {
                NormalTransaction transaction = transactionBucket.get(transactionId);
                transaction.pushRedo(normalRedo);
                long redoSize = transaction.getSize();
                if (redoSize % transaction.getLargeTransactionUpperLimit() == 0) {
                    tapLogger.info(TapLog.CON_LOG_0008.getMsg(), transactionId, redoSize);
                }
                break;
            }
            case "COMMIT":
                if (transactionBucket.containsKey(transactionId)) {
                    NormalTransaction transaction = transactionBucket.get(transactionId);
                    transaction.setLastTimestamp(normalRedo.getTimestamp());
                    commitTransaction(redoConsumer, transaction);
                } else {
                    LinkedList<NormalRedo> redoes = new LinkedList<>();
                    redoes.add(normalRedo);
                    NormalTransaction transaction = new NormalTransaction(cdcSequenceId, transactionId, redoes);
                    transaction.setLargeTransactionUpperLimit(largeTransactionUpperLimit);
                    transaction.setConnectorId(connectorId);
                    transaction.incrementSize(1);
                    transaction.setLastTimestamp(normalRedo.getTimestamp());
                    Map<String, NormalTransaction> cacheCommitTraction = new HashMap<>();
                    cacheCommitTraction.put(transactionId, transaction);
                    redoConsumer.accept(cacheCommitTraction);
                }
                break;
            case "DDL": {
                LinkedList<NormalRedo> redoes = new LinkedList<>();
                redoes.add(normalRedo);
                NormalTransaction transaction = new NormalTransaction(cdcSequenceId, transactionId, redoes);
                transaction.setLargeTransactionUpperLimit(largeTransactionUpperLimit);
                transaction.setConnectorId(connectorId);
                transaction.incrementSize(1);
                transaction.setLastTimestamp(normalRedo.getTimestamp());
                Map<String, NormalTransaction> cacheCommitTraction = new HashMap<>();
                cacheCommitTraction.put(transactionId, transaction);
                redoConsumer.accept(cacheCommitTraction);
                break;
            }
            case "ROLLBACK": {
                if (transactionBucket.containsKey(transactionId)) {
                    NormalTransaction transaction = transactionBucket.get(transactionId);
                    if (transaction.isLarge()) {
                        tapLogger.debug("Found large transaction be rolled back: {}", transaction);
                    }
                    transaction.clearRedoes();
                    transactionBucket.remove(transactionId);
                }
                break;
            }
            default:
                break;
        }
    }

    protected void sendTransaction(Map<String, NormalTransaction> txMap) {
        for (Map.Entry<String, NormalTransaction> txEntry : txMap.entrySet()) {
            NormalTransaction logTransaction = txEntry.getValue();
            AtomicReference<List<TapEvent>> eventList = new AtomicReference<>(TapSimplify.list());
            AtomicReference<NormalRedo> lastRedo = new AtomicReference<>();
            NormalRedo redoTemp;
            while ((redoTemp = logTransaction.pollRedo()) != null) {
                createEvent(redoTemp, eventList, logTransaction.getLastTimestamp());
                if (eventList.get().size() >= 1000) {
                    submitEvent(redoTemp, eventList.get());
                    eventList.set(TapSimplify.list());
                }
                lastRedo.set(redoTemp);
            }
            submitEvent(lastRedo.get(), eventList.get());
            txEntry.getValue().clearRedoes();
        }
    }

    protected abstract void ddlFlush() throws Throwable;

    protected abstract void createEvent(NormalRedo normalRedo, AtomicReference<List<TapEvent>> eventList, long timestamp);

    protected abstract void submitEvent(NormalRedo normalRedo, List<TapEvent> list);

    private void commitTransaction(Consumer<Map<String, NormalTransaction>> redoConsumer, NormalTransaction transaction) {
        final String transactionId = transaction.getTransactionId();
        transactionBucket.remove(transactionId);
        long redoSize = transaction.getSize();
        if (redoSize >= transaction.getLargeTransactionUpperLimit()) {
            tapLogger.info(TapLog.D_CONN_LOG_0002.getMsg(), transactionId, redoSize);
        } else {
            tapLogger.debug(TapLog.D_CONN_LOG_0002.getMsg(), transactionId, redoSize);
        }
        Map<String, NormalTransaction> cacheCommitTraction = new HashMap<>();
        cacheCommitTraction.put(transactionId, transaction);
        redoConsumer.accept(cacheCommitTraction);
    }
}
