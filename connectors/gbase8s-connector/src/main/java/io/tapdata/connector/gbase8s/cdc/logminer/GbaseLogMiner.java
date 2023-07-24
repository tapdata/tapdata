package io.tapdata.connector.gbase8s.cdc.logminer;

import com.gbasedbt.jdbcx.IfxConnectionPoolDataSource;
import com.gbasedbt.jdbcx.IfxDataSource;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.IfxCDCEngine;
import com.informix.stream.cdc.records.IfxCDCCommitTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCOperationRecord;
import io.tapdata.common.cdc.ILogMiner;
import io.tapdata.common.cdc.LogMiner;
import io.tapdata.common.cdc.LogTransaction;
import io.tapdata.common.cdc.RedoLogContent;
import io.tapdata.connector.gbase8s.Gbase8sJdbcContext;
import io.tapdata.connector.gbase8s.cdc.Gbase8sOffset;
import io.tapdata.connector.gbase8s.config.Gbase8sConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.springframework.util.Assert;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.informix.stream.api.IfmxStreamRecordType.AFTER_UPDATE;

/**
 * Author:Skeet
 * Date: 2023/6/16
 **/
public class GbaseLogMiner extends LogMiner implements ILogMiner {

    public Gbase8sJdbcContext gbase8sJdbcContext;
    public Gbase8sConfig gbase8sConfig;
    public Gbase8sOffset gbase8sOffset;
    private IfxCDCEngine ifxCDCEngine;
    private IfxCDCEngine engine;
    private final Map<Integer, String> labelTableMap = new HashMap<>();
    private final Map<Integer, String> labelSchemaMap = new HashMap<>();
    private Long lastEventTimestamp = 0L;

    public GbaseLogMiner(Gbase8sJdbcContext gbase8sJdbcContext, String connectorId, Log logMiner) {
        super();
    }

    @Override
    public void startMiner() throws Throwable {
        isRunning.set(true);
        //初始化重做日志队列和线程
        Gbase8sConfig cdcConfig = (Gbase8sConfig) gbase8sConfig.copy();
        cdcConfig.setDatabase("syscdcv1");
        IfxConnectionPoolDataSource dataSource = new IfxConnectionPoolDataSource();
        dataSource.setServerName(cdcConfig.getDatabaseUrl());
        dataSource.setUser(cdcConfig.getUser());
        dataSource.setPassword(cdcConfig.getPassword());
        IfxDataSource ds = new IfxDataSource(dataSource);
        IfxCDCEngine.Builder builder = new IfxCDCEngine.Builder(ds);
        String tableFormat = "\"%s\":\"%s\".\"%s\"";
        tableList.forEach(table -> builder.watchTable(String.format(tableFormat, gbase8sConfig.getDatabase(), gbase8sConfig.getSchema(), table),
                tableMap.get(table).getNameFieldMap().keySet().stream().map(t -> "\"" + t + "\"").toArray(String[]::new)));
        builder.timeout(5);
        builder.sequenceId(gbase8sOffset.getPendingScn());
        engine = builder.build();
        builder.getWatchedTables().forEach(table -> {
            labelTableMap.put(table.getLabel(), StringKit.removeHeadTail(table.getTableName(), "\"", null));
            labelSchemaMap.put(table.getLabel(), StringKit.removeHeadTail(table.getNamespace(), "\"", null));
        });
        engine.init();
        IfmxStreamRecord record;
        RedoLogContent redoLogContent = null;
        while (isRunning.get() && (record = engine.getRecord()) != null) {
            if (EmptyKit.isNotNull(threadException.get())) {
                throw new RuntimeException(threadException.get());
            }
            if (record.getType() != AFTER_UPDATE) {
                redoLogContent = new RedoLogContent();
            }
            Assert.notNull(redoLogContent, "redoLogContent can not be null");
            redoLogContent.setOperation(String.valueOf(record.getType()));
            switch (record.getType()) {
                case INSERT:
                case DELETE: {
                    Map<String, Object> data = new HashMap<>(((IfxCDCOperationRecord) record).getData());
                    redoLogContent.setRedoRecord(data);
                    break;
                }
                case AFTER_UPDATE: {
                    Map<String, Object> data = new HashMap<>(((IfxCDCOperationRecord) record).getData());
                    redoLogContent.setRedoRecord(data);
                    redoLogContent.setOperation("UPDATE");
                    break;
                }
                case BEFORE_UPDATE: {
                    Map<String, Object> data = new HashMap<>(((IfxCDCOperationRecord) record).getData());
                    redoLogContent.setUndoRecord(data);
                    continue;
                }
                case COMMIT:
                case ROLLBACK:
                    break;
                case DISCARD:
                    redoLogContent.setOperation("ROLLBACK");
                    break;
                case METADATA:
                    // TODO: 2023/6/4 ddl need to be supported
//                    redoLogContent.setOperation("DDL");
//                    redoLogContent.setRedoRecord(TapSimplify.map(TapSimplify.entry("ddl", ((IfxCDCMetaDataRecord) record).getColumns())));
//                    break;
                case ERROR:
                case TRUNCATE:
                case TRANSACTION_GROUP:
                    tapLogger.warn("find recordType {} Check the sequenceId: {}", record.getType(), record.getSequenceId());
                    continue;
                case BEGIN:
                case TIMEOUT:
                    continue;
                default:
                    throw new IllegalStateException("Unexpected value: " + record.getType());
            }
            redoLogContent.setScn(record.getSequenceId());
            redoLogContent.setTimestamp(new Timestamp(0));
            if (record instanceof IfxCDCCommitTransactionRecord) {
                lastEventTimestamp = ((IfxCDCCommitTransactionRecord) record).getTime();
                redoLogContent.setTimestamp(new Timestamp(lastEventTimestamp));
            }
            if (EmptyKit.isNotNull(record.getLabel())) {
                redoLogContent.setTableName(labelTableMap.get(Integer.valueOf(record.getLabel())));
                redoLogContent.setSegOwner(labelSchemaMap.get(Integer.valueOf(record.getLabel())));
            }
            redoLogContent.setXid(String.valueOf(record.getTransactionId()));
            // TODO: 2023/6/4 big transaction need to be supported
            redoLogContent.setRsId(String.valueOf(record.getTransactionId()));
            enqueueRedoLogContent(redoLogContent);
        }
    }
    @Override
    protected void ddlFlush() throws Throwable {

    }

    @Override
    protected void batchCreateEvents(List<RedoLogContent> redoLogContentList, AtomicReference<List<TapEvent>> eventList, AtomicReference<RedoLogContent> lastRedoLogContent, long timestamp) {

    }

    @Override
    protected void submitEvent(RedoLogContent redoLogContent, List<TapEvent> list) {
        Gbase8sOffset gbase8sOffset = new Gbase8sOffset();
        assert redoLogContent != null;
        gbase8sOffset.setLastScn(redoLogContent.getScn());
        Iterator<LogTransaction> iterator = transactionBucket.values().iterator();
        if (iterator.hasNext()) {
            gbase8sOffset.setPendingScn(iterator.next().getRacMinimalScn());
        } else {
            gbase8sOffset.setPendingScn(redoLogContent.getScn());
        }
        if (list.size() > 0) {
            consumer.accept(list, gbase8sOffset);
        }
    }

    @Override
    public void stopMiner() throws Throwable {
        tapLogger.info("Log Miner is shutting down...");
        isRunning.set(false);
        Optional.ofNullable(redoLogConsumerThreadPool).ifPresent(ExecutorService::shutdown);
        redoLogConsumerThreadPool = null;
    }
}
