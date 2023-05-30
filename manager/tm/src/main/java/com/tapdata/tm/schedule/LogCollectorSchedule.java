package com.tapdata.tm.schedule;

import com.tapdata.tm.task.service.LogCollectorService;
import lombok.Setter;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Setter(onMethod_ = {@Autowired})
public class LogCollectorSchedule {

	private LogCollectorService logCollectorService;

	@Scheduled(cron = "0 0 0/2 * * ?")
	@SchedulerLock(name ="task_log_collector_clear", lockAtMostFor = "10s", lockAtLeastFor = "10s")
	public void schedule() {
		logCollectorService.clear();
	}

	@Scheduled(cron = "0 0/30 * * * ?")
	@SchedulerLock(name ="task_log_collector_remove", lockAtMostFor = "10s", lockAtLeastFor = "10s")
	public void removeTaskSchedule() {
		logCollectorService.removeTask();
	}
}
