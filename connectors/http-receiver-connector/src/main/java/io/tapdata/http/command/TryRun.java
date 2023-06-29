package io.tapdata.http.command;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.http.HttpReceiverConnector;
import io.tapdata.http.command.entity.CollectLog;
import io.tapdata.http.entity.ConnectionConfig;
import io.tapdata.http.util.ListUtil;
import io.tapdata.http.util.ScriptEvel;
import io.tapdata.http.util.Tags;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

/**
 * @author GavinXiao
 * @description TryRun create by Gavin
 * @create 2023/5/24 16:00
 **/
public class TryRun implements Command {
    public static final String TAG = TryRun.class.getSimpleName();
    public static final String EVENT_DATA_KEY = "before";
    public static final String LOG_COUNT_KEY = "logSize";
    private ScriptEngine scriptEngine;
    TapConnectionContext tapConnectionContext;

    /**
     * {
     *      "reqId":"",
     *      "ts": 111111111111,
     *      "data":{
     *           "logs":[{},{}],
     *           "before":[{},{}],
     *           "after":[{},{}],
     *           "script:""
     *      },
     *      "code":"ok/error"
     * }
     */
    @Override
    public CommandResult execCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {
        this.tapConnectionContext = tapConnectionContext;
        ConnectionConfig config = null == tapConnectionContext ||  null == tapConnectionContext.getConnectionConfig()?
                ConnectionConfig.create(commandInfo.getConnectionConfig())
                : ConnectionConfig.create(tapConnectionContext);
        if (!config.handleType()) {
            throw new CoreException("Can not test run, data processing script not enabled");
        }
        if (null == tapConnectionContext) {
            throw new CoreException("Can not test run, connection config is empty");
        }
        return handleCommand(tapConnectionContext, commandInfo, config);
    }

    private CommandResult handleCommand(final TapConnectionContext context, final CommandInfo commandInfo, final ConnectionConfig config) {
        Map<String, Object> commandArgs = Optional.ofNullable(commandInfo.getArgMap()).orElse(new HashMap<>());
        final int logSize = (Integer) Optional.ofNullable(commandArgs.get(LOG_COUNT_KEY)).orElse(100);
        List<CollectLog.LogRecord> logList = new LinkedList<CollectLog.LogRecord>() {
            private static final int MAX_SIZE = 100;
            private boolean overflow = false;
            private final int count = logSize <= 0 || logSize > MAX_SIZE? MAX_SIZE : logSize;

            @Override
            public boolean add(CollectLog.LogRecord o) {
                if (overflow) {
                    return true;
                }
                if (size() > count) {
                    this.overflow = true;
                    return super.add(new CollectLog.LogRecord("ERROR",
                            "The log exceeds the maximum limit, ignore the following logs.", System.currentTimeMillis()));
                }
                return super.add(o);
            }

        };
        List<Object> afterData = new ArrayList<>();
        List<Object> beforeData = new ArrayList<>();
        final CollectLog<? extends Log> logger = new CollectLog<>(context.getLog(), logList);
        Map<String, Object> resultDate = new HashMap<>();
        resultDate.put("logs", logList);
        resultDate.put("after", afterData);
        resultDate.put("script", config.script());
        CommandResult commandResult = new CommandResult();
        commandResult.setData(resultDate);
        //commandResult.setCode(CommandResult.CODE_OK);
        try {
            Object eventObj = commandArgs.get(EVENT_DATA_KEY);
            if (null == eventObj) {
                logger.warn("[{}] No third-party push events received", TAG);
                eventObj = new ArrayList<>();
            }
            resultDate.put(EVENT_DATA_KEY, ListUtil.addObjToList(beforeData, eventObj));
            logger.info("[{}] Start executing command [TestRun] ", TAG);
            testRun(beforeData, afterData, commandInfo, config, logger);
            logger.info("[{}] Command [TestRun] execution complete.", TAG);
        } catch (Exception e) {
            CoreException coreException = new CoreException(123, e, e.getMessage());
            coreException.setData(commandResult);
            logger.warn(e.getMessage());
            throw coreException;
//            commandResult.setCode(CommandResult.CODE_ERROR);
//            commandResult.setMsg(e.getMessage());
        }
        return commandResult;
    }

    private void testRun(
            final List<Object> beforeData,
            final List<Object> afterData,
            final CommandInfo commandInfo,
            final ConnectionConfig config,
            final CollectLog<? extends Log> logger) {
        String script = config.script();
        final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
        scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));
        if (null != scriptEngine) {
            try {
                ScriptEvel scriptEvel = ScriptEvel.create(scriptEngine, this.tapConnectionContext);
                scriptEvel.evalSourceForSelf();
                scriptEngine.eval(script);
            } catch (Exception e) {
                //logger.warn("[{}] Can not get event handle script, please check you connection config. msg: {}", TAG, e.getMessage());
                throw new CoreException(e.getMessage());
            }
            Invocable invocable = (Invocable) scriptEngine;
            beforeData.stream().filter(Objects::nonNull).forEach(data -> {
                try {
                    ListUtil.addObjToList(afterData,
                            invocable.invokeFunction(
                                ConnectionConfig.EVENT_FUNCTION_NAME,
                                data
                            ));
                } catch (ScriptException e) {
                    logger.warn("[{}] Occur exception When execute script, error message: {}", TAG, e.getMessage());
                    throw new CoreException(e.getMessage(), e);
                } catch (NoSuchMethodException methodException) {
                    logger.warn("[{}] Occur exception When execute script, error message: Can not find function named is '{}' in script.", TAG, ConnectionConfig.EVENT_FUNCTION_NAME);
                    throw new CoreException(methodException.getMessage(), methodException);
                }
            });
        } else {
            logger.warn("[{}] Can not get event handle script, please check you connection config.", TAG);
            throw new CoreException("[{}] Can not get event handle script, please check you connection config.", TAG);
        }
    }

}
