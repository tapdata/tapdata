package io.tapdata.task;

import com.tapdata.entity.ScheduleTask;

import java.util.concurrent.ScheduledFuture;

public class TaskExecuteInfo {

	private ScheduledFuture scheduledFuture;

	private ScheduleTask scheduleTask;

	public TaskExecuteInfo(ScheduledFuture scheduledFuture, ScheduleTask scheduleTask) {
		this.scheduledFuture = scheduledFuture;
		this.scheduleTask = scheduleTask;
	}

	public ScheduledFuture getScheduledFuture() {
		return scheduledFuture;
	}

	public void setScheduledFuture(ScheduledFuture scheduledFuture) {
		this.scheduledFuture = scheduledFuture;
	}

	public ScheduleTask getScheduleTask() {
		return scheduleTask;
	}

	public void setScheduleTask(ScheduleTask scheduleTask) {
		this.scheduleTask = scheduleTask;
	}
}
