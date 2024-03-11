package io.tapdata.Schedule.Watcher;

import com.tapdata.constant.BeanUtil;
import io.tapdata.Application;
import io.tapdata.common.SettingService;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import io.tapdata.observable.logging.util.LogUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.core.appender.rolling.action.Action;
import org.apache.logging.log4j.core.appender.rolling.action.DeleteAction;
import java.util.Collection;
import java.util.Map;

import static io.tapdata.Schedule.LogConfigurationWatcherManager.AGENT;
import static io.tapdata.observable.logging.util.LogUtil.logLevel;

public class AgentLogConfigurationWatcher extends AbstractLogConfigurationWatcher {
    protected LoggerContext context = LoggerContext.getContext(false);
    protected static final  String HTTPAPPENDER="httpAppender";

    public AgentLogConfigurationWatcher(LogConfiguration agentLogConfiguration) {
        super(agentLogConfiguration);
    }

    public AgentLogConfigurationWatcher() {
    }

    @Override
    LogConfiguration getLogConfig() {
        SettingService settingService = BeanUtil.getBean(SettingService.class);
        LogConfiguration logConfiguration = ObsLoggerFactory.getInstance().getLogConfiguration(AGENT);
        String scriptEngineHttpApender = settingService.getString("scriptEngineHttpAppender", "false");
        String logLevel = settingService.getString("logLevel","info");
        logConfiguration.setLogLevel(logLevel);
        logConfiguration.setScriptEngineHttpAppender(scriptEngineHttpApender);
        return logConfiguration;
    }

    @Override
    protected boolean checkIsModify(LogConfiguration logConfiguration) {
        boolean upperCheck =  super.checkIsModify(logConfiguration);
        return upperCheck || (null != logConfiguration.getLogLevel() && !logConfiguration.getLogLevel().equals(this.logConfiguration.getLogLevel()))
                || (null != logConfiguration.getScriptEngineHttpAppender() && !logConfiguration.getScriptEngineHttpAppender().equals(this.logConfiguration.getScriptEngineHttpAppender()));
    }

    @Override
    protected void updateConfig(LogConfiguration logConfiguration) {
        updateRollingFileAppender(logConfiguration);
        updateLogLevel(logConfiguration);
    }

    public void updateLogLevel(LogConfiguration logConfiguration) {
        String logLevel = logConfiguration.getLogLevel();
        String scriptEngineHttpAppender = logConfiguration.getScriptEngineHttpAppender();
        Level level = logLevel(logLevel);

        Collection<Logger> loggers = context.getLoggers();
        for (org.apache.logging.log4j.core.Logger logger1 : loggers) {
            final String loggerName = logger1.getName();
            if (
                    StringUtils.startsWithIgnoreCase(loggerName, "io.tapdata") ||
                            StringUtils.startsWithIgnoreCase(loggerName, "com.tapdata")
            ) {
                logger1.setLevel(level);
                if (StringUtils.contains(loggerName, "CustomProcessor")) {
                    final Map<String, Appender> appenders = logger1.get().getAppenders();
                    refreshAppenders(logger1, scriptEngineHttpAppender, appenders);
                }
            }
        }
    }

    protected void refreshAppenders(Logger logger1, String scriptEngineHttpAppender, Map<String, Appender> appenders) {
        if ("false".equals(scriptEngineHttpAppender)) {
            if (appenders.containsKey(HTTPAPPENDER)) {
                logger1.setAdditive(false);
                final Map<String, Appender> rootAppenders = context.getRootLogger().getAppenders();
                for (Appender appender : rootAppenders.values()) {
                    logger1.addAppender(appender);
                }
                logger1.get().removeAppender(HTTPAPPENDER);
            }
        } else if (!appenders.containsKey(HTTPAPPENDER)) {
            logger1.setAdditive(true);
        }
    }

    private void updateRollingFileAppender(LogConfiguration logConfiguration) {
        org.apache.logging.log4j.core.config.Configuration config = context.getConfiguration();
        Appender appender = context.getRootLogger().getAppenders().get("rollingFileAppender");
        RollingFileAppender rollingFileAppender = null;
        if (appender instanceof RollingFileAppender) {
            rollingFileAppender = (RollingFileAppender) appender;
        }
        if(null == rollingFileAppender){
            return;
        }
        RollingFileManager manager = rollingFileAppender.getManager();
        CompositeTriggeringPolicy compositeTriggeringPolicy = LogUtil.getCompositeTriggeringPolicy(logConfiguration.getLogSaveSize().toString());
        String glob = "tapdata-agent-*.log.*.gz";
        DeleteAction deleteAction = LogUtil.getDeleteAction(logConfiguration.getLogSaveTime(), Application.logsPath, glob, config);
        Action[] actions = {deleteAction};
        DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
                .withMax(logConfiguration.getLogSaveCount().toString())
                .withCustomActions(actions)
                .withConfig(config)
                .build();
        manager.setRolloverStrategy(strategy);
        manager.setTriggeringPolicy(compositeTriggeringPolicy);
    }
}
