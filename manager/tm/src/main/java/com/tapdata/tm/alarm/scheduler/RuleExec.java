package com.tapdata.tm.alarm.scheduler;

import com.tapdata.tm.alarm.service.impl.AlarmServiceImpl;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import io.tapdata.common.executor.ExecutorsManager;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Dexter
 */
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class RuleExec {
    private RuleRenew alertRuleRenew;
    private AlarmServiceImpl alarmService;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ExecutorService executorService = ExecutorsManager.getInstance().getExecutorService();


//    @Async
//    @Scheduled(fixedDelay = 10000)
    public void alertRulesPeriodicallyExec() {
        for (Map.Entry<String, Rule> entry : alertRuleRenew.getExecutableRules().entrySet()) {
            Rule rule = entry.getValue();
            if (rule.shouldExec()) {
                executorService.submit(() -> {
                    try {
                        execRule(rule);
                    } catch (Throwable throwable) {
                        log.warn("failed to execute the rule: {}, err: {}", rule, throwable.getMessage(), throwable);
                    }
                });
            }
        }

    }

    public void stop() {
        running.set(false);
    }

    private void execRule(Rule rule) {
        if (!running.get()) {
            return;
        }

        // query the last 1m metrics data
        long end  = System.currentTimeMillis();
        long start = end - 60000;





    }

}
