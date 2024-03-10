package io.tapdata.Schedule;

import io.tapdata.Schedule.Watcher.AgentLogConfigurationWatcher;
import io.tapdata.Schedule.Watcher.TaskLogConfigurationWatcher;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
@Component
public class LogConfigurationWatcherManager {
    public static final String AGENT = "agent";
    public static final String TASK = "task";

    private static final ScheduledExecutorService logConfigurationResetScheduledThreadPool = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Log-Configuration-Scheduled-Runner"));
    public static final int LOG_CONFIG_SCHEDULED_DELAY = 10;
    public static final int LOG_CONFIG_SCHEDULED_PERIOD=10;

    @PostConstruct
    void start() {
        AgentLogConfigurationWatcher agentLogConfigurationWatcher = new AgentLogConfigurationWatcher();
        TaskLogConfigurationWatcher taskLogConfigurationWatcher = new TaskLogConfigurationWatcher();
        logConfigurationResetScheduledThreadPool.scheduleAtFixedRate(() -> {
            agentLogConfigurationWatcher.onCheck();
            taskLogConfigurationWatcher.onCheck();
        }, LOG_CONFIG_SCHEDULED_DELAY, LOG_CONFIG_SCHEDULED_PERIOD, TimeUnit.SECONDS);
    }



}
