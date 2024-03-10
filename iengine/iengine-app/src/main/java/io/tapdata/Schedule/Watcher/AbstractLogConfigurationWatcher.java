package io.tapdata.Schedule.Watcher;

import io.tapdata.observable.logging.util.Conf.LogConfiguration;

public abstract class AbstractLogConfigurationWatcher {
    protected LogConfiguration logConfiguration;

    protected AbstractLogConfigurationWatcher(LogConfiguration logConfiguration) {
        this.logConfiguration = logConfiguration;
    }

    protected AbstractLogConfigurationWatcher() {

    }

    public void onCheck() {
        LogConfiguration logConfiguration = getLogConfig();
        if (checkIsModify(logConfiguration)) {
            this.logConfiguration = logConfiguration;
            updateConfig(logConfiguration);
        }
    }
    abstract LogConfiguration getLogConfig();

    protected boolean checkIsModify(LogConfiguration logConfiguration) {
        if (null != logConfiguration) {
            return null == this.logConfiguration || (null != logConfiguration.getLogSaveCount() && !logConfiguration.getLogSaveCount().equals(this.logConfiguration.getLogSaveCount()))
                    || (null != logConfiguration.getLogSaveSize() && !logConfiguration.getLogSaveSize().equals(this.logConfiguration.getLogSaveSize()))
                    || (null != logConfiguration.getLogSaveTime() && !logConfiguration.getLogSaveTime().equals(this.logConfiguration.getLogSaveTime()));
        }
        return false;
    }

    protected abstract void updateConfig(LogConfiguration logConfiguration);
}
