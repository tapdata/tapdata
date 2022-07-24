package io.tapdata.task;

import com.tapdata.entity.ScheduleTask;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;

import java.util.Map;

public class TaskContext {

	public final static String META_DB_URI_ARG = "$META_DB_URI";
	public final static String META_DB_PARAMS = "$META_DB_PARAMS";

	private ClientMongoOperator clientMongoOperator;

	private Map<String, Object> tapdataBulitInData;

	private Map<String, Object> taskData;

	private ScheduleTask scheduleTask;

	private SettingService settingService;

	public TaskContext(ClientMongoOperator clientMongoOperator, SettingService settingService) {
		this.clientMongoOperator = clientMongoOperator;
		this.settingService = settingService;
	}

	public TaskContext(ScheduleTask task, Map<String, Object> tapdataBulitInData, ClientMongoOperator clientMongoOperator, SettingService settingService) {
		taskData = task.getTask_data();
		this.tapdataBulitInData = tapdataBulitInData;
		this.scheduleTask = task;
		this.clientMongoOperator = clientMongoOperator;
		this.settingService = settingService;
	}

	public Map<String, Object> getTapdataBulitInData() {
		return tapdataBulitInData;
	}

	public Map<String, Object> getTaskData() {
		return taskData;
	}

	public ScheduleTask getScheduleTask() {
		return scheduleTask;
	}

	public void setScheduleTask(ScheduleTask scheduleTask) {
		this.scheduleTask = scheduleTask;
	}

	public ClientMongoOperator getClientMongoOperator() {
		return clientMongoOperator;
	}

	public void setClientMongoOperator(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	public SettingService getSettingService() {
		return settingService;
	}

	public void setSettingService(SettingService settingService) {
		this.settingService = settingService;
	}
}
