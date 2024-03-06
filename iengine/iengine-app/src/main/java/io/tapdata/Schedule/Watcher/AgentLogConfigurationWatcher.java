package io.tapdata.Schedule.Watcher;

import io.tapdata.Application;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.appender.AppenderFactory;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.core.appender.rolling.action.Action;
import org.apache.logging.log4j.core.appender.rolling.action.DeleteAction;

import static io.tapdata.Schedule.LogConfigurationWatcherManager.AGENT;

public class AgentLogConfigurationWatcher extends AbstractLogConfigurationWatcher {
    protected LoggerContext context = LoggerContext.getContext(false);
    private AppenderFactory appenderFactory= AppenderFactory.getInstance();

    public AgentLogConfigurationWatcher(LogConfiguration agentLogConfiguration) {
        super(agentLogConfiguration);
    }

    public AgentLogConfigurationWatcher() {
    }

    @Override
    LogConfiguration getLogConfig() {
        LogConfiguration logConfiguration = ObsLoggerFactory.getInstance().getLogConfiguration(AGENT);
        return logConfiguration;
    }

    @Override
    protected void updateConfig(LogConfiguration logConfiguration) {
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
        CompositeTriggeringPolicy compositeTriggeringPolicy = appenderFactory.getCompositeTriggeringPolicy(logConfiguration.getLogSaveSize().toString());
        String glob = "tapdata-agent-*.log.*.gz";
        DeleteAction deleteAction = appenderFactory.getDeleteAction(logConfiguration.getLogSaveTime(), Application.logsPath, glob, config);
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
