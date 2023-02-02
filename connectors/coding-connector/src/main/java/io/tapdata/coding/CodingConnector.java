package io.tapdata.coding;

import cn.hutool.http.HttpRequest;
import io.tapdata.base.ConnectorBase;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.Param;
import io.tapdata.coding.enums.CodingEvent;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.service.command.Command;
import io.tapdata.coding.service.loader.*;
import io.tapdata.coding.service.connectionMode.CSVMode;
import io.tapdata.coding.service.connectionMode.ConnectionMode;
import io.tapdata.coding.service.schema.SchemaStart;
import io.tapdata.coding.utils.collection.MapUtil;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.LastData;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.coding.enums.TapEventTypes.*;
import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.map;

@TapConnectorClass("spec.json")
public class CodingConnector extends ConnectorBase {
    private static final String TAG = CodingConnector.class.getSimpleName();

    private final Object streamReadLock = new Object();
    private final long streamExecutionGap = 5000;//util: ms
    private int batchReadPageSize = 500;//coding page 1~500,

    private Long lastTimePoint;
    private List<Integer> lastTimeSplitIssueCode = new ArrayList<>();//hash code list

    private LastData lastCommandResult;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        IssuesLoader.create(connectionContext).verifyConnectionConfig();
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        String streamReadType = connectionConfig.getString("streamReadType");
        if (Checker.isEmpty(streamReadType)) {
            throw new CoreException("Error in connection parameter [streamReadType], please go to verify");
        }
        switch (streamReadType) {
            //反向赋空，如果使用webhook那么取消polling能力，如果使用polling南无取消webhook能力.
            case "WebHook":
                this.connectorFunctions.supportStreamRead(null);
                break;
            case "Polling":
                this.connectorFunctions.supportRawDataCallbackFilterFunctionV2(null);
                break;
//			default:
//				throw new CoreException("Error in connection parameters [streamReadType],just be [WebHook] or [Polling], please go to verify");
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        synchronized (this) {
            this.notify();
        }
        TapLogger.info(TAG, "Stop connector");
    }

    private ConnectorFunctions connectorFunctions;

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportBatchRead(this::batchRead)
                .supportBatchCount(this::batchCount)
                .supportTimestampToStreamOffset(this::timestampToStreamOffset)
                .supportStreamRead(this::streamRead)
                .supportRawDataCallbackFilterFunctionV2(this::rawDataCallbackFilterFunctionV2)
                .supportCommandCallbackFunction(this::handleCommand)
                .supportMemoryFetcherV2(this::memoryFetcher)
        ;
        this.connectorFunctions = connectorFunctions;
    }

    private DataMap memoryFetcher(String keyRegex, String memoryLevel) {
        return DataMap.create().keyRegex(keyRegex)
                .kv("streamExecutionGap", streamExecutionGap)
                .kv("batchReadPageSize", batchReadPageSize)
                .kv("lastCommandResult", lastCommandResult)
                .kv("lastTimePoint", lastTimePoint)
                ;
    }

    private CommandResult handleCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {
        return LastData.traceLastData(() -> Command.command(tapConnectionContext, commandInfo), lastData -> this.lastCommandResult = lastData);
    }

    private List<TapEvent> rawDataCallbackFilterFunctionV2(TapConnectorContext connectorContext, List<String> tableList, Map<String, Object> issueEventData) {
        //CodingLoader<Param> loader = CodingLoader.loader(connectorContext, "");
        //return Checker.isNotEmpty(loader) ? loader.rawDataCallbackFilterFunction(issueEventData) : null;
        List<CodingLoader<Param>> loaders = CodingLoader.loader(connectorContext, tableList);
        if (Checker.isNotEmpty(loaders) && !loaders.isEmpty()) {
            List<TapEvent> events = new ArrayList<TapEvent>() {{
                for (CodingLoader<Param> loader : loaders) {
                    List<TapEvent> tapEvents = loader.rawDataCallbackFilterFunction(issueEventData);
                    if (Checker.isNotEmpty(tapEvents) && !tapEvents.isEmpty()) {
                        addAll(tapEvents);
                    }
                }
            }};
            return !events.isEmpty() ? events : null;
        }
        return null;
    }

    public void streamRead(
            TapConnectorContext nodeContext,
            List<String> tableList,
            Object offsetState,
            int recordSize,
            StreamReadConsumer consumer) {
        List<CodingLoader<Param>> loaders = CodingLoader.loader(nodeContext, tableList);
        if (Checker.isEmpty(loaders) || loaders.isEmpty()) {
            throw new CoreException("can not load CodingLoad, please sure your table name is accurate.");
        }
        consumer.streamReadStarted();
        while (isAlive()) {
            synchronized (this) {
                try {
                    this.wait(loaders.get(0).streamReadTime());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for (CodingLoader<Param> loader : loaders) {
                loader.streamRead(tableList, offsetState, recordSize, consumer);
            }
        }
    }


    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
        Long date = time != null ? time : System.currentTimeMillis();
        List<SchemaStart> allSchemas = SchemaStart.getAllSchemas(tapConnectorContext);
        return CodingOffset.create(allSchemas.stream().collect(Collectors.toMap(
                schema -> ((SchemaStart) schema).tableName(),
                schema -> date
        )));
    }

    /**
     * Batch read
     *
     * @param connectorContext
     * @param table
     * @param offset
     * @param batchCount
     * @param consumer
     * @auth GavinX
     */
    public void batchRead(TapConnectorContext connectorContext,
                          TapTable table,
                          Object offset,
                          int batchCount,
                          BiConsumer<List<TapEvent>, Object> consumer) {
        TapLogger.debug(TAG, "start {} batch read", table.getName());
        CodingLoader<Param> loader = CodingLoader.loader(connectorContext, table.getId());
        if (Checker.isNotEmpty(loader)) {
            loader.connectorInit(this);
            loader.batchRead(offset, batchCount, consumer);
            loader.connectorOut();
        }
        TapLogger.debug(TAG, "compile {} batch read", table.getName());
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        CodingLoader<Param> loader = CodingLoader.loader(tapConnectorContext, tapTable.getId());
        if (Checker.isNotEmpty(loader)) {
            int count = loader.batchCount();
            return Long.parseLong(String.valueOf(count));
        }
        TapLogger.debug(TAG, "batchCountV2 = 0", tapTable.getId());
        return 0L;
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        //return TapTable for each project. Issue
        //IssueLoader.create(connectionContext).setTableSize(tableSize).discoverIssue(tables,consumer);
        String modeName = connectionContext.getConnectionConfig().getString("connectionMode");
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(connectionContext, modeName);
        if (null == connectionMode) {
            throw new CoreException("Connection Mode is not empty or not null.");
        }
        List<TapTable> tapTables = connectionMode.discoverSchema(tables, tableSize);
        List<TapTable> tablesFinal = new ArrayList<>();
        if (null != tapTables && !tapTables.isEmpty()) {
            tapTables.stream().filter(Objects::nonNull).forEach(tab -> {
                String tabId = tab.getId();
                if (null == tables || tables.isEmpty() || tables.contains(tabId)) {
                    tablesFinal.add(tab);
                }
            });
        }
        if (null != tapTables) {
            consumer.accept(tablesFinal.subList(0, Math.min(tablesFinal.size(), tableSize)));
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();

        TestCoding testConnection = TestCoding.create(connectionContext);
        TestItem testItem = testConnection.testItemConnection();
        consumer.accept(testItem);
        if (testItem.getResult() == TestItem.RESULT_FAILED) {
            return connectionOptions;
        }

        TestItem testToken = testConnection.testToken();
        consumer.accept(testToken);
        if (testToken.getResult() == TestItem.RESULT_FAILED) {
            return connectionOptions;
        }

        TestItem testProject = testConnection.testProject();
        consumer.accept(testProject);
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        List<SchemaStart> allSchemas = SchemaStart.getAllSchemas(connectionContext);
        return allSchemas.size();
    }
}
