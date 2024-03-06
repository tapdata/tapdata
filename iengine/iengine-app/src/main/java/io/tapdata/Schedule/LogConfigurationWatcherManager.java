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

    private static final ScheduledExecutorService taskResetRetryServiceScheduledThreadPool = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Log-Configuration-Scheduled-Runner"));

    @PostConstruct
    void start() {
        AgentLogConfigurationWatcher agentLogConfigurationWatcher = new AgentLogConfigurationWatcher();
        TaskLogConfigurationWatcher taskLogConfigurationWatcher = new TaskLogConfigurationWatcher();
        taskResetRetryServiceScheduledThreadPool.scheduleAtFixedRate(() -> {
            agentLogConfigurationWatcher.onCheck();
            taskLogConfigurationWatcher.onCheck();
        }, 10, 10, TimeUnit.SECONDS);
    }



}
