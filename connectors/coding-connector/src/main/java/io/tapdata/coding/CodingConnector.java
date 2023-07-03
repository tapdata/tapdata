package io.tapdata.coding;

import io.tapdata.base.ConnectorBase;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.param.Param;
import io.tapdata.coding.service.command.Command;
import io.tapdata.coding.service.connectionMode.ConnectionMode;
import io.tapdata.coding.service.loader.CodingLoader;
import io.tapdata.coding.service.loader.CodingStarter;
import io.tapdata.coding.service.loader.IssuesLoader;
import io.tapdata.coding.service.loader.TestCoding;
import io.tapdata.coding.service.schema.SchemaStart;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.ErrorHttpException;
import io.tapdata.coding.utils.http.InterceptorHttp;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.LastData;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec-oauth.json")
public class CodingConnector extends ConnectorBase {
    private static final String TAG = CodingConnector.class.getSimpleName();

    private final Object streamReadLock = new Object();
    private final long streamExecutionGap = 5000;//util: ms
    private int batchReadPageSize = 500;//coding page 1~500,

    private Set<String> lastTimeSplitIssueCode = new HashSet<>();//hash code list
    private long issuesLastTimePoint;
    private Set<String> lastTimeSplitIterationCode = new HashSet<>();//hash code list
    private long iterationsLastTimePoint;
    private Set<String> lastTimeProjectMembersCode = new HashSet<>();//hash code list

    private LastData lastCommandResult;
    private final AtomicReference<String> accessToken = new AtomicReference<>();

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        IssuesLoader loader = IssuesLoader.create(connectionContext, accessToken);
        loader.verifyConnectionConfig();
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
            //default:
            //	throw new CoreException("Error in connection parameters [streamReadType],just be [WebHook] or [Polling], please go to verify");
        }
        CodingHttp.interceptor = (http, request, hasIgnore) -> {
            if (hasIgnore) return request;
            try {
                Map<?,?> body = fromJson(request.body(),Map.class);
                Map<?, ?> response = (Map<?, ?>)body.get("Response");
                response = (Map<?, ?>)response.get("Error");
                String code = (String)response.get("Code");
                String message = (String)response.get("Message");
                if ("ResourceNotFound".equals(code)|| " User not found, authorization invalid".equals(message)) {
                    return http.header("Authorization", loader.refreshTokenByOAuth2()).execute();
                }
            }catch (Exception ignored){
            }
            return request;
        };
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        synchronized (this) {
            this.notify();
        }
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
                .supportErrorHandleFunction(this::error)
                .supportMemoryFetcherV2(this::memoryFetcher)
        ;
        this.connectorFunctions = connectorFunctions;
    }

    private RetryOptions error(TapConnectionContext context, PDKMethod pdkMethod, Throwable throwable) {
        Throwable lastCause = ErrorKit.getLastCause(throwable);
        if (lastCause instanceof ErrorHttpException || lastCause instanceof IOException) {
            return RetryOptions.create().needRetry(true).beforeRetryMethod(() -> {
                try {
                    synchronized (this.streamReadLock) {
                        this.onStop(context);
                        this.onStart(context);
                    }
                } catch (Throwable e) {
                    TapLogger.warn("Cannot stop and start Coding connector when occur an http error or IOException. {}", e.getMessage());
                }
            });
        }
        return null;
    }

    private DataMap memoryFetcher(String keyRegex, String memoryLevel) {
        return DataMap.create().keyRegex(keyRegex)
                .kv("streamExecutionGap", streamExecutionGap)
                .kv("batchReadPageSize", batchReadPageSize)
                .kv("lastCommandResult", lastCommandResult)
                ;
    }

    private CommandResult handleCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {
        tapConnectionContext.setConnectionConfig(new DataMap(){{Optional.ofNullable(commandInfo.getConnectionConfig()).ifPresent(this::putAll);}});
        tapConnectionContext.setNodeConfig(new DataMap(){{Optional.ofNullable(commandInfo.getNodeConfig()).ifPresent(this::putAll); }});
        IssuesLoader loader = new IssuesLoader(tapConnectionContext, accessToken);
        CodingHttp.interceptor = (http, request, hasIgnore) -> {
            if (hasIgnore) return request;
            try {
                Map<?,?> body = fromJson(request.body(), Map.class);
                Map<?, ?> response = (Map<?, ?>)body.get("Response");
                response = (Map<?, ?>)response.get("Error");
                String code = (String)response.get("Code");
                String message = (String)response.get("Message");
                if ("ResourceNotFound".equals(code)|| " User not found, authorization invalid".equals(message)) {
                    return http.header("Authorization", loader.refreshTokenByOAuth2()).execute();
                }
            }catch (Exception ignored){
            }
            return request;
        };
        return LastData.traceLastData(() -> Command.command(tapConnectionContext, commandInfo, accessToken), lastData -> this.lastCommandResult = lastData);
    }

    private List<TapEvent> rawDataCallbackFilterFunctionV2(TapConnectorContext connectorContext, List<String> tableList, Map<String, Object> issueEventData) {
        //CodingLoader<Param> loader = CodingLoader.loader(connectorContext, "");
        //return Checker.isNotEmpty(loader) ? loader.rawDataCallbackFilterFunction(issueEventData) : null;
        List<CodingLoader<Param>> loaders = CodingLoader.loader(connectorContext, tableList, accessToken);
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
        List<CodingLoader<Param>> loaders = CodingLoader.loader(nodeContext, tableList, accessToken);
        if (Checker.isEmpty(loaders) || loaders.isEmpty()) {
            throw new CoreException("can not load CodingLoad, please sure your table name is accurate.");
        }
        consumer.streamReadStarted();
        while (isAlive()) {
            synchronized (this) {
                try {
                    this.wait(2 * 60 * 1000L);
//                    this.wait(loaders.get(0).streamReadTime());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for (CodingLoader<Param> loader : loaders) {
                loader.connectorInit(this);
                loader.streamRead(tableList, offsetState, recordSize, consumer);
                loader.connectorOut();
            }
        }
        consumer.streamReadEnded();
    }


    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
        Long date = time != null ? time : System.currentTimeMillis();
        List<SchemaStart> allSchemas = SchemaStart.getAllSchemas(tapConnectorContext, accessToken);
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
        CodingLoader<Param> loader = CodingLoader.loader(connectorContext, table.getId(), accessToken);
        if (Checker.isNotEmpty(loader)) {
            loader.connectorInit(this);
            loader.batchRead(offset, batchCount, consumer);
            loader.connectorOut();
        }
        TapLogger.debug(TAG, "compile {} batch read", table.getName());
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        CodingLoader<Param> loader = CodingLoader.loader(tapConnectorContext, tapTable.getId(), accessToken);
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
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(connectionContext, accessToken, modeName);
        if (null == connectionMode) {
            throw new CoreException("Connection Mode is not empty or not null.");
        }
        List<TapTable> tapTables = connectionMode.discoverSchema(tables, tableSize, accessToken);
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
        TestCoding testConnection = TestCoding.create(connectionContext, accessToken);
        testConnection.veryContextConfigAndNodeConfig();
        CodingHttp.interceptor = (http, request, hasIgnore) -> {
            if (hasIgnore) return request;
            try {
                Map<?,?> body = fromJson(request.body(), Map.class);
                Map<?, ?> response = (Map<?, ?>)body.get("Response");
                response = (Map<?, ?>)response.get("Error");
                String code = (String)response.get("Code");
                String message = (String)response.get("Message");
                if ("ResourceNotFound".equals(code)|| " User not found, authorization invalid".equals(message)) {
                    return http.header("Authorization", testConnection.refreshTokenByOAuth2()).execute();
                }
            }catch (Exception ignored){
            }
            return request;
        };
        ConnectionOptions connectionOptions = ConnectionOptions.create();
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
        if (TestItem.RESULT_SUCCESSFULLY == testProject.getResult()) {
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
        }
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        List<SchemaStart> allSchemas = SchemaStart.getAllSchemas(connectionContext, accessToken);
        return allSchemas.size();
    }

    public Set<String> lastTimeSplitIssueCode(){
        return this.lastTimeSplitIssueCode;
    }
    public void lastTimeSplitIssueCode(Set<String> set){
        this.lastTimeSplitIssueCode = set;
    }
    public Set<String> lastTimeSplitIterationCode(){
        return this.lastTimeSplitIterationCode;
    }
    public void lastTimeSplitIterationCode(Set<String> set){
        this.lastTimeSplitIterationCode = set;
    }
    public Set<String> lastTimeProjectMembersCode(){
        return this.lastTimeProjectMembersCode;
    }
    public void lastTimeProjectMembersCode(Set<String> set){
        this.lastTimeProjectMembersCode = set;
    }
    public long iterationsLastTimePoint(){
        return this.iterationsLastTimePoint;
    }
    public void iterationsLastTimePoint(long iterationsLastTimePoint){
        this.iterationsLastTimePoint = iterationsLastTimePoint;
    }
    public long issuesLastTimePoint(){
        return this.issuesLastTimePoint;
    }
    public void issuesLastTimePoint(long issuesLastTimePoint){
        this.issuesLastTimePoint = issuesLastTimePoint;
    }
}
