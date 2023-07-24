package io.tapdata.connector.gbase8s.cdc.logminer;

import com.gbase8s.stream.cdc.Gbase8sCdc;
import com.gbase8s.stream.cdc.IfxCDCEngine;
import com.informix.jdbc.IfxObject;
import io.tapdata.common.cdc.*;
import io.tapdata.connector.gbase8s.Gbase8sJdbcContext;
import io.tapdata.connector.gbase8s.cdc.Gbase8sOffset;
import io.tapdata.connector.gbase8s.config.Gbase8sConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.sql.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Author:Skeet
 * Date: 2023/6/16
 **/
public class Gbase8sNewLogMiner extends NormalLogMiner implements ILogMiner {

    protected final Gbase8sJdbcContext gbase8sJdbcContext;
    protected final Gbase8sConfig gbase8sConfig;
    protected Gbase8sOffset gbase8sOffset;
    private IfxCDCEngine engine;
    private final Map<Integer, String> labelTableMap = new HashMap<>();
    private final Map<Integer, String> labelSchemaMap = new HashMap<>();
    private Long lastEventTimestamp = 0L;

    public Gbase8sNewLogMiner(Gbase8sJdbcContext gbase8sJdbcContext, String connectorId, Log tapLogger) {
        this.gbase8sJdbcContext = gbase8sJdbcContext;
        this.gbase8sConfig = (Gbase8sConfig) gbase8sJdbcContext.getConfig();
        this.connectorId = connectorId;
        this.tapLogger = tapLogger;
        this.setLargeTransactionUpperLimit(10000);
    }


    @Override
    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        super.init(tableList, tableMap, offsetState, recordSize, consumer);
        gbase8sOffset = (Gbase8sOffset) offsetState;
    }

    @Override
    public void startMiner() throws Throwable {
        isRunning.set(true);
        initRedoLogQueueAndThread();
        Gbase8sConfig cdcConfig = (Gbase8sConfig) gbase8sConfig.copy();
        cdcConfig.setDatabase("syscdcv1");
        Connection connection = DriverManager.getConnection(gbase8sConfig.getDatabaseUrl(), gbase8sConfig.getUser(), gbase8sConfig.getPassword());
        Gbase8sCdc gbase8sCdc = new Gbase8sCdc(connection);
        gbase8sCdc.init();
        //获取智能大对象前置操作
        //获取智能大对象
//        getSmartBlob();


//
//        IfxCDCEngine ifxCDCEngine = new IfxCDCEngine(connection);
//        List<IfmxStreamRecord> records;
//        IfxDataSource ds = new IfxDataSource(cdcConfig.getDatabaseUrl() + ";user=" + cdcConfig.getUser() + ";password=" + cdcConfig.getPassword() + ";");
//        IfxCDCEngine.Builder builder = new IfxCDCEngine.Builder(ds);
//        String tableFormat = "\"%s\":\"%s\".\"%s\"";
//        tableList.forEach(table -> builder.watchTable(String.format(tableFormat, gbase8sConfig.getDatabase(), gbase8sConfig.getSchema(), table),
//                tableMap.get(table).getNameFieldMap().keySet().stream().map(t -> "\"" + t + "\"").toArray(String[]::new)));
//        builder.timeout(60);
//        builder.buffer(102400000);
//        builder.sequenceId(gbase8sOffset.getPendingScn());
//        engine = builder.build();
//        engine.setTapLogger(tapLogger);
//        engine.setFetchSize(1);
//        builder.getWatchedTables().forEach(table -> {
//            labelTableMap.put(table.getLabel(), StringKit.removeHeadTail(table.getTableName(), "\"", null));
//            labelSchemaMap.put(table.getLabel(), StringKit.removeHeadTail(table.getNamespace(), "\"", null));
//        });
//        engine.init();
//        List<IfmxStreamRecord> records;
//        NormalRedo normalRedo = null;
//        while (isRunning.get() && EmptyKit.isNotEmpty(records = engine.getRecords())) {
//            if (EmptyKit.isNotNull(threadException.get())) {
//                throw new RuntimeException(threadException.get());
//            }
//            for (IfmxStreamRecord record : records) {
//                if (record.getType() != AFTER_UPDATE) {
//                    normalRedo = new NormalRedo();
//                }
//                Assert.notNull(normalRedo, "redoLogContent can not be null");
//                normalRedo.setOperation(String.valueOf(record.getType()));
//                switch (record.getType()) {
//                    case INSERT:
//                    case DELETE: {
//                        Map<String, Object> data = new HashMap<>(((IfxCDCOperationRecord) record).getData());
//                        normalRedo.setRedoRecord(data);
//                        break;
//                    }
//                    case AFTER_UPDATE: {
//                        Map<String, Object> data = new HashMap<>(((IfxCDCOperationRecord) record).getData());
//                        normalRedo.setRedoRecord(data);
//                        normalRedo.setOperation("UPDATE");
//                        break;
//                    }
//                    case BEFORE_UPDATE: {
//                        Map<String, Object> data = new HashMap<>(((IfxCDCOperationRecord) record).getData());
//                        normalRedo.setUndoRecord(data);
//                        continue;
//                    }
//                    case BEGIN:
//                    case COMMIT:
//                    case ROLLBACK:
//                    case TIMEOUT:
//                        break;
//                    case DISCARD:
//                        normalRedo.setOperation("ROLLBACK");
//                        break;
//                    case METADATA:
//                        // TODO: 2023/6/4 ddl need to be supported
////                    redoLogContent.setOperation("DDL");
////                    redoLogContent.setRedoRecord(TapSimplify.map(TapSimplify.entry("ddl", ((IfxCDCMetaDataRecord) record).getColumns())));
////                    break;
//                    case ERROR:
//                    case TRUNCATE:
//                    case TRANSACTION_GROUP:
//                        tapLogger.warn("find recordType {} Check the sequenceId: {}", record.getType(), record.getSequenceId());
//                        continue;
//                    default:
//                        throw new IllegalStateException("Unexpected value: " + record.getType());
//                }
//                normalRedo.setCdcSequenceId(record.getSequenceId());
//                normalRedo.setTimestamp(0L);
//                if (record instanceof IfxCDCCommitTransactionRecord) {
//                    lastEventTimestamp = ((IfxCDCCommitTransactionRecord) record).getTime() * 1000;
//                    normalRedo.setTimestamp(lastEventTimestamp);
//                }
//                if (record instanceof IfxCDCBeginTransactionRecord) {
//                    lastEventTimestamp = ((IfxCDCBeginTransactionRecord) record).getTime() * 1000;
//                    normalRedo.setTimestamp(lastEventTimestamp);
//                }
//                if (EmptyKit.isNotNull(record.getLabel())) {
//                    normalRedo.setTableName(labelTableMap.get(Integer.valueOf(record.getLabel())));
//                    normalRedo.setNameSpace(labelSchemaMap.get(Integer.valueOf(record.getLabel())));
//                }
//                normalRedo.setTransactionId(String.valueOf(record.getTransactionId()));
//                // TODO: 2023/6/4 big transaction need to be supported
//                enqueueRedoLogContent(normalRedo);
//
//            }

        }
//    }

//    private void getSmartBlob() throws SQLException {
//        //获取只能大对象
//        byte[] handle = new byte[1024];
//        //获取智能大对象的句柄数据
//        ByteBuffer byteBuffer = ByteBuffer.wrap(handle);
//        //构造智能大对象句柄对象
//        IfxLocator loPtr = new IfxLocator(handle);
//        //构造一个 java 智能对象类，用于读取智能大对象的数据
//        IfxSmartBlob smb =new IfxSmartBlob(connection);
//        //使用智能大对象的句柄，打开智能大对象，返回值为大对象的描述符
//        int loFd = smb.IfxLoOpen(loPtr, IfxSmartBlob.LO_RDONLY);
//        if (loFd >= 0)
//        {
//            //获取智能大对象的状态
//            IfxLoStat loStat =smb.IfxLoGetStat(loFd);
//
//            byte[] readBuffer = new byte[(int)loStat.getSize()];
//
//            int number = smb.IfxLoRead(loFd, readBuffer, (int)loStat.getSize());
//            smb.IfxLoClose(loFd);
//        }
//    }

    private void initRedoLogQueueAndThread() {
        if (redoLogConsumerThreadPool == null) {
            redoLogConsumerThreadPool = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            redoLogConsumerThreadPool.submit(() -> {
                NormalRedo normalRedo;
                while (isRunning.get()) {
                    while (ddlStop.get()) {
                        TapSimplify.sleep(1000);
                    }
                    try {
                        normalRedo = logQueue.poll(1, TimeUnit.SECONDS);
                        if (normalRedo == null) {
                            continue;
                        }
                    } catch (Exception e) {
                        break;
                    }
                    try {
                        if ("TIMEOUT".equals(normalRedo.getOperation())) {
                            consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime((System.currentTimeMillis()))), gbase8sOffset);
                        }
                        convertDataToJavaType(normalRedo);
                        processOrBuffRedo(normalRedo, this::sendTransaction);
                    } catch (Throwable e) {
                        threadException.set(e);
                        consumer.streamReadEnded();
                    }
                }
            });

            redoLogConsumerThreadPool.submit(() -> {
                try {
                    while (isRunning.get()) {
                        Iterator<String> iterator = transactionBucket.keySet().iterator();
                        while (iterator.hasNext()) {
                            String transactionId = iterator.next();
                            NormalTransaction transaction = transactionBucket.get(transactionId);
                            if (lastEventTimestamp - transaction.getFirstTimestamp() < 720 * 60 * 1000L) {
                                break;
                            } else {
                                tapLogger.warn("Uncommitted transaction {} with {} events will be dropped", transactionId, transaction.getSize());
                                transaction.clearRedoes();
                                iterator.remove();
                            }
                        }
                        int sleep = 60;
                        try {
                            while (isRunning.get() && (sleep-- > 0)) {
                                TapSimplify.sleep(1000);
                            }
                        } catch (Exception ignore) {
                        }
                    }
                } catch (Exception exception) {
                    threadException.set(exception);
                }
            });
        }
    }

    private void convertDataToJavaType(NormalRedo normalRedo) throws SQLException {
        if (normalRedo.getRedoRecord() != null) {
            Map<String, Object> redoRecord = normalRedo.getUndoRecord();
            for (Map.Entry<String, Object> entry : redoRecord.entrySet()) {
                parseKeyValue(entry);
            }
        }
        if (normalRedo.getUndoRecord() != null) {
            Map<String, Object> undoRecord = normalRedo.getUndoRecord();
            for (Map.Entry<String, Object> entry : undoRecord.entrySet()) {
                parseKeyValue(entry);
            }
        }
    }

    private void parseKeyValue(Map.Entry<String, Object> entry) throws SQLException {
        if (EmptyKit.isNull(entry.getValue())) {
            return;
        }
        entry.setValue(((IfxObject) entry.getValue()).toObject());
//        switch (entry.getValue().getClass().getSimpleName()) {
//            case "IfxChar":
//            case "IfxVarChar":
//            case "IfxLvarchar":
//                entry.setValue(entry.getValue().toString());
//                break;
//            case "IfxBoolean":
//                entry.setValue(((IfxBoolean) entry.getValue()).toBoolean());
//                break;
//            case "IfxBigInt":
//            case "IfxInt8":
//                entry.setValue(Long.valueOf(entry.getValue().toString()));
//                break;
//            case "IfxInteger":
//            case "IfxShort":
//                entry.setValue(Integer.valueOf(entry.getValue().toString()));
//                break;
//            case "IfxBigDecimal":
//        }
    }

    protected void ddlFlush() throws Throwable {

    }

    protected void createEvent(NormalRedo normalRedo, AtomicReference<List<TapEvent>> eventList, long timestamp) {
        if (EmptyKit.isNull(Objects.requireNonNull(normalRedo).getRedoRecord())) {
            return;
        }
        TapRecordEvent recordEvent;
        switch (Objects.requireNonNull(normalRedo).getOperation()) {
            case "INSERT": {
                recordEvent = new TapInsertRecordEvent().init()
                        .table(normalRedo.getTableName())
                        .after(normalRedo.getRedoRecord());
                break;
            }

            case "UPDATE": {
                recordEvent = new TapUpdateRecordEvent().init()
                        .table(normalRedo.getTableName())
                        .after(normalRedo.getRedoRecord())
                        .before(normalRedo.getUndoRecord());
                break;
            }

            case "DELETE": {
                recordEvent = new TapDeleteRecordEvent().init()
                        .table(normalRedo.getTableName())
                        .before(normalRedo.getRedoRecord());
                break;
            }

            default:
                return;
        }

        recordEvent.setReferenceTime(timestamp);
        eventList.get().add(recordEvent);
    }

    protected void submitEvent(NormalRedo normalRedo, List<TapEvent> list) {
        assert normalRedo != null;
        gbase8sOffset.setLastScn(normalRedo.getCdcSequenceId());
        Iterator<NormalTransaction> iterator = transactionBucket.values().iterator();
        if (iterator.hasNext()) {
            gbase8sOffset.setPendingScn(iterator.next().getCdcSequenceId());
        } else {
            gbase8sOffset.setPendingScn(normalRedo.getCdcSequenceId());
        }
        if (list.size() > 0) {
            consumer.accept(list, gbase8sOffset);
        }
    }


    @Override
    public void stopMiner() throws Throwable {
        super.stopMiner();
        if (EmptyKit.isNotNull(engine)) {
            engine.close();
        }
    }
}
