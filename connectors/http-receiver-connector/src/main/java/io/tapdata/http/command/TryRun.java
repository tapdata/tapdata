package io.tapdata.http.command;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.http.entity.ConnectionConfig;
import io.tapdata.http.util.ScriptEvel;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.tapdata.base.ConnectorBase.toJson;

/**
 * @author GavinXiao
 * @description TryRun create by Gavin
 * @create 2023/5/24 16:00
 **/
public class TryRun implements Command {
    public static final String TAG = TryRun.class.getSimpleName();
    public static final String EVENT_DATA_KEY = "event";
    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
    private ScriptEngine scriptEngine;

    @Override
    public CommandResult execCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {
        ConnectionConfig config = null == tapConnectionContext ?
                ConnectionConfig.create(commandInfo.getConnectionConfig())
                : ConnectionConfig.create(tapConnectionContext);
        String script = config.script();
        Map<String, Object> commandArgs = Optional.ofNullable(commandInfo.getArgMap()).orElse(new HashMap<>());
        Object eventObj = commandArgs.get(EVENT_DATA_KEY);
        if (null == eventObj){
            TapLogger.warn(TAG, "No third-party push events received");
            return Command.emptyResult();
            //throw new CoreException("No third-party push events received");
        }
        TapLogger.info(TAG, "third-party push event: {}", toJson(eventObj));
        scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));
        if (null != scriptEngine) {
            try {
                ScriptEvel scriptEvel = ScriptEvel.create(scriptEngine);
                scriptEvel.evalSourceForSelf();
                scriptEngine.eval(script);
            }catch (Exception e){
                throw new CoreException("Can not get event handle script, please check you connection config.");
            }
            Invocable invocable = (Invocable) scriptEngine;
            try {
                Object invokeResult = invocable.invokeFunction(ConnectionConfig.EVENT_FUNCTION_NAME, eventObj);
                //@TODO return Result

            } catch (ScriptException e) {
                TapLogger.warn(TAG, "Occur exception When execute script, error message: {}", e.getMessage());
            } catch (NoSuchMethodException methodException) {
                TapLogger.warn(TAG, "Occur exception When execute script, error message: Can not find function named is '{}' in script.", ConnectionConfig.EVENT_FUNCTION_NAME);
            }
        } else {
            throw new CoreException("Can not get event handle script, please check you connection config.");
        }
        return Command.emptyResult();
    }
}
