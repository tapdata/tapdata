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
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.connection.CommandCallbackFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class JSCommandFunction extends FunctionBase implements FunctionSupport<CommandCallbackFunction> {
    private static final String TAG = JSCommandFunction.class.getSimpleName();

    private JSCommandFunction() {
        super();
        super.functionName = JSFunctionNames.CommandV1;
    }

    @Override
    public CommandCallbackFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) {
            return null;
        }
        return this::commandV1;
//        if (super.hasNotSupport(javaScripter)){
//            return !super.javaScripter.functioned(JSFunctionNames.CommandV1.jsName()) ? null : this::commandV1;
//        }else {
//            return !super.javaScripter.functioned(JSFunctionNames.CommandV1.jsName()) ? this::commandV3 : this::commandV2;
//        }
    }

    private CommandResult commandV3(TapConnectionContext context, CommandInfo commandInfo) {
        return null;
    }

    private CommandResult commandV2(TapConnectionContext context, CommandInfo commandInfo) {
        CommandResult commandResult = new CommandResult();
        if (Objects.isNull(context)) {
            throw new CoreException("TapConnectorContext cannot not be empty.");
        }
        if (Objects.isNull(commandInfo)) {
            throw new CoreException("Command info cannot be empty.");
        }
        try {
            Object invoker;
            synchronized (JSConnector.execLock) {
                invoker = super.javaScripter.invoker(
                        JSFunctionNames.CommandV1.jsName(),
                        Optional.ofNullable(context.getConnectionConfig()).orElse(new DataMap()),
                        commandInfo
                );
            }
            if (Objects.isNull(invoker)) {
                Map<String, Object> data = new HashMap<>();
                data.put("data", invoker);
                commandResult.result(data);
            } else {
                synchronized (JSConnector.execLock) {
                    invoker = super.javaScripter.invoker(
                            JSFunctionNames.CommandV2.jsName(),
                            invoker
                    );
                }
                if (invoker instanceof Map) {
                    commandResult.result((Map<String, Object>) invoker);
                } else {
                    Map<String, Object> data = new HashMap<>();
                    data.put("data", invoker);
                    commandResult.result(data);
                }
            }
        } catch (Exception e) {
            throw new CoreException("Method 'command' failed to execute. Unable to get the return result. The final result will be null ,msg: {}", InstanceFactory.instance(TapUtils.class).getStackTrace(e));
        }
        return commandResult;
    }

    private CommandResult commandV1(TapConnectionContext context, CommandInfo commandInfo) {
        CommandResult commandResult = new CommandResult();
        if (Objects.isNull(commandInfo)) {
            throw new CoreException("Command info cannot be empty.");
        }
        Map<String, Object> commandMap = new HashMap<>();
        commandMap.put("command", commandInfo.getCommand());
        commandMap.put("argMap", commandInfo.getArgMap());
        commandMap.put("action", commandInfo.getAction());
        commandMap.put("type", commandInfo.getType());
        commandMap.put("time", commandInfo.getTime());
        Map<String, Object> configMap = ExecuteConfig.contextConfig(null)
                .connection(commandInfo.getConnectionConfig())
                .node(commandInfo.getNodeConfig())
                .toMap();
        super.javaScripter.put("_tapConfig_", configMap);
        JSAPIInterceptorConfig config = JSAPIInterceptorConfig.config();
        JSAPIResponseInterceptor interceptor = JSAPIResponseInterceptor.create(config).configMap(configMap);
        DataMap connections = new DataMap();
        DataMap nodes = new DataMap();
        connections.putAll(Optional.ofNullable(commandInfo.getConnectionConfig()).orElse(new HashMap<>()));
        connections.putAll(Optional.ofNullable(commandInfo.getNodeConfig()).orElse(new HashMap<>()));
        TapConnectionContext contextTemp = new TapConnectionContext(new TapNodeSpecification(), connections, nodes, new TapLog());
        interceptor.updateToken(BaseUpdateTokenFunction.create(this.javaScripter, contextTemp));
        Object tapAPI = this.javaScripter.scriptEngine().get("tapAPI");
        this.javaScripter.scriptEngine().put("tapAPI", ((APIFactoryDecorator) tapAPI).interceptor(interceptor));
        this.javaScripter.scriptEngine().put("tapLog", new TapConnectorLog(contextTemp.getLog()));
        try {
            Object invoker;
            synchronized (JSConnector.execLock) {
                invoker = super.javaScripter.invoker(
                        this.functionName.jsName(),
                        commandInfo.getConnectionConfig(),
                        commandInfo.getNodeConfig(),
                        commandMap
                );
            }
            if (invoker instanceof Map) {
                commandResult.result((Map<String, Object>) invoker);
            } else {
                Map<String, Object> data = new HashMap<>();
                data.put("data", invoker);
                commandResult.result(data);
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, " Method " + this.functionName.jsName() + " failed to execute. Unable to get the return result. The final result will be null. " + InstanceFactory.instance(TapUtils.class).getStackTrace(e));
            throw e;
        }
        return commandResult;
    }

    public static CommandCallbackFunction create(LoadJavaScripter loadJavaScripter) {
        return new JSCommandFunction().function(loadJavaScripter);
    }
}
