package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.base.TapConnectorLog;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.decorator.APIFactoryDecorator;
import io.tapdata.js.connector.server.function.ExecuteConfig;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.js.connector.server.inteceptor.JSAPIInterceptorConfig;
import io.tapdata.js.connector.server.inteceptor.JSAPIResponseInterceptor;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author GavinXiao
 * @description JSExecuteCommandFunction create by Gavin
 * @create 2023/6/9 11:48
 **/
public class JSExecuteCommandFunction extends FunctionBase implements FunctionSupport<ExecuteCommandFunction> {
    private final static String TAG = JSExecuteCommandFunction.class.getSimpleName();

    private JSExecuteCommandFunction() {
        super();
        super.functionName = JSFunctionNames.ExecuteCommandFunction;
    }

    @Override
    public ExecuteCommandFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) {
            return null;
        }
        return this::execute;
    }

    private void execute(TapConnectorContext context, TapExecuteCommand command, Consumer<ExecuteResult> consumer) {
        ExecuteResult<Object> commandResult = new ExecuteResult<>();
        if (Objects.isNull(command)) {
            throw new CoreException("Command info cannot be empty.");
        }
        Map<String, Object> commandMap = new HashMap<>();
        commandMap.put("command", command.getCommand());
        commandMap.put("params", command.getParams());
        commandMap.put("batchSize", command.getBatchSize());

//        Map<String, Object> configMap = ExecuteConfig.contextConfig(null)
//                .connection(commandInfo.getConnectionConfig())
//                .node(commandInfo.getNodeConfig())
//                .toMap();
//        super.javaScripter.put("_tapConfig_", configMap);
//        JSAPIInterceptorConfig config = JSAPIInterceptorConfig.config();
//        JSAPIResponseInterceptor interceptor = JSAPIResponseInterceptor.create(config).configMap(configMap);
//        DataMap connections = new DataMap();
//        DataMap nodes = new DataMap();
//        connections.putAll(Optional.ofNullable(commandInfo.getConnectionConfig()).orElse(new HashMap<>()));
//        connections.putAll(Optional.ofNullable(commandInfo.getNodeConfig()).orElse(new HashMap<>()));
//        TapConnectionContext contextTemp = new TapConnectionContext(new TapNodeSpecification(), connections, nodes, new TapLog());
//        interceptor.updateToken(BaseUpdateTokenFunction.create(this.javaScripter, contextTemp));
//        Object tapAPI = this.javaScripter.scriptEngine().get("tapAPI");
//        this.javaScripter.scriptEngine().put("tapAPI", ((APIFactoryDecorator) tapAPI).interceptor(interceptor));
//        this.javaScripter.scriptEngine().put("tapLog", new TapConnectorLog(contextTemp.getLog()));

        try {
            Object invoker;
            synchronized (JSConnector.execLock) {
                invoker = super.javaScripter.invoker(
                        this.functionName.jsName(),
                        Optional.ofNullable(context.getConnectionConfig()).orElse(new DataMap()),
                        Optional.ofNullable(context.getNodeConfig()).orElse(new DataMap()),
                        commandMap
                );
            }
            commandResult.result(LoadJavaScripter.covertData(invoker));
        } catch (Exception e) {
            TapLogger.warn(TAG, "Method " + this.functionName.jsName() + " failed to execute. Unable to get the return result. The final result will be null. " + InstanceFactory.instance(TapUtils.class).getStackTrace(e));
            throw e;
        }finally {
            consumer.accept(commandResult);
        }
    }

    public static ExecuteCommandFunction create(LoadJavaScripter loadJavaScripter) {
        return new JSExecuteCommandFunction().function(loadJavaScripter);
    }
}
