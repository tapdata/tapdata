package io.tapdata.http;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.http.command.Command;
import io.tapdata.http.entity.ConnectionConfig;
import io.tapdata.http.receiver.ConnectionTest;
import io.tapdata.http.receiver.EventHandle;
import io.tapdata.http.util.Checker;
import io.tapdata.http.util.ScriptEvel;
import io.tapdata.http.util.Tags;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.BiConsumer;

/**
 * @author GavinXiao
 * @description HttpReceiverConnector create by Gavin
 * @create 2023/5/17 12:20
 **/
@TapConnectorClass("spec.json")
public class HttpReceiverConnector extends ConnectorBase {
    public static final String TAG = HttpReceiverConnector.class.getSimpleName();
    private ScriptEngine scriptEngine;
    private ConnectionConfig config;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
        scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));
        config = ConnectionConfig.create(connectionContext);
        if (config.handleType()) {
            if (null != scriptEngine) {
                ScriptEvel scriptEvel = ScriptEvel.create(scriptEngine, connectionContext);
                scriptEvel.evalSourceForSelf();
                scriptEngine.eval(config.script());
            } else {
                throw new CoreException("Can not get event handle script, please check you connection config.");
            }
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry tapCodecsRegistry) {
        connectorFunctions
                .supportBatchRead(this::batchRead)
                //.supportStreamRead(this::streamRead)
                .supportTimestampToStreamOffset(this::offset)
                .supportRawDataCallbackFilterFunctionV2(this::callback)
                .supportCommandCallbackFunction(this::handleCommand)
        ;
    }

    //final Object waitLock = new Object();
    //private void streamRead(TapConnectorContext context, List<String> tables, Object offset, int size, StreamReadConsumer consumer) {
    //    consumer.streamReadStarted();
    //    try {
    //        while (isAlive()){
    //            try {
    //                waitLock.wait(1000L);
    //            }catch (Exception ignore){
    //            }
    //        }
    //    }finally {
    //        waitLock.notifyAll();
    //    }
    //    consumer.streamReadEnded();
    //}

    private Object offset(TapConnectorContext context, Long time) {
        if (null != time) {
            return time;
        }
        return System.currentTimeMillis();
    }

    private void batchRead(TapConnectorContext context, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> con) {
        context.getLog().info("Http Receiver can not support batch read, and batch read is over now.");
    }

    private List<TapEvent> callback(TapConnectorContext context, List<String> tableName, Map<String, Object> eventMap) {
        if (null == config) {
            config = ConnectionConfig.create(context);
        }
        String name = config.tableName();
        if (Checker.isEmpty(eventMap)) {
            context.getLog().debug("WebHook of http body is empty, Data callback has been over.");
            return null;
        }
        Object data = Tags.filterCallbackEvent(context, eventMap);
        if (null == data) {
            return null;
        }
        Object supplierKey = eventMap.get("proxy_callback_supplier_id");
        //Object listObj = eventMap.get("array");
        //if (Checker.isEmpty(listObj) || !(listObj instanceof Collection)){
        //    TapLogger.debug(TAG,"WebHook of http body is empty or not Collection, Data callback has been over.");
        //    return null;
        //}
        //List<Map<String,Object>> dataEventList = (List<Map<String, Object>>)listObj;
        if (!config.handleType()){
           return EventHandle.eventList(name, data);
        }

        if (null != scriptEngine) {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                Object invokeResult = invocable.invokeFunction(
                        ConnectionConfig.EVENT_FUNCTION_NAME,
                        data,
                        supplierKey
                );
                if (null != invokeResult) {
                    return EventHandle.eventList(name, invokeResult);
                }
                context.getLog().info("After script filtering, the current record has been ignored. Please be informed, record is {}", toJson(eventMap));
            } catch (ScriptException e) {
                context.getLog().warn("Occur exception When execute script, error message: {}", e.getMessage());
            } catch (NoSuchMethodException methodException) {
                context.getLog().warn("Occur exception When execute script, error message: Can not find function named is '{}' in script.", ConnectionConfig.EVENT_FUNCTION_NAME);
            }
        } else {
            throw new CoreException("Can not get script engine, please check you connection config.");
        }
        return null;
    }

    @Override
    public void discoverSchema(TapConnectionContext tapConnectionContext, List<String> list, int i, Consumer<List<TapTable>> consumer) throws Throwable {
        ConnectionConfig config = ConnectionConfig.create(tapConnectionContext);
        TapTable tapTable = new TapTable(config.getTableName(), config.getTableName());
        tapTable.setNameFieldMap(new LinkedHashMap<>());
        consumer.accept(list(tapTable));
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext tapConnectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionTest test = ConnectionTest.create(ConnectionConfig.create(tapConnectionContext));
        ConnectionOptions options = ConnectionOptions.create();
        consumer.accept(test.testTableName());
        //consumer.accept(test.testHookUrl());
        //consumer.accept(test.testScript());
        return options;
    }

    @Override
    public int tableCount(TapConnectionContext tapConnectionContext) throws Throwable {
        return 1;
    }

    private CommandResult handleCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {
        //tapConnectionContext.setConnectionConfig(new DataMap(){{
        //    Optional.ofNullable(commandInfo.getConnectionConfig()).ifPresent(this::putAll);}});
        //tapConnectionContext.setNodeConfig(new DataMap(){{Optional.ofNullable(commandInfo.getNodeConfig()).ifPresent(this::putAll); }});
        return Command.command(tapConnectionContext, commandInfo);
    }
}
