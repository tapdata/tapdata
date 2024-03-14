package io.tapdata.Schedule.Watcher;

import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.TaskLogger;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;

import java.util.Map;

import static io.tapdata.Schedule.LogConfigurationWatcherManager.TASK;

public class TaskLogConfigurationWatcher extends AbstractLogConfigurationWatcher {

    public TaskLogConfigurationWatcher() {
        super();
    }

    @Override
    LogConfiguration getLogConfig() {
        LogConfiguration logConfiguration = ObsLoggerFactory.getInstance().getLogConfiguration(TASK);
        return logConfiguration;
    }

    public TaskLogConfigurationWatcher(LogConfiguration taskLogConfiguration) {
        super(taskLogConfiguration);
    }


    @Override
    protected void updateConfig(LogConfiguration logConfiguration) {
        Map<String, TaskLogger> taskLoggersMap = ObsLoggerFactory.getInstance().getTaskLoggersMap();
        taskLoggersMap.values().forEach(taskLogger -> {
            taskLogger.refreshFileAppender(logConfiguration);
        });
    }
}