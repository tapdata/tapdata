package io.tapdata.task;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.AgentUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.PkgAnnoUtil;
import com.tapdata.entity.ScheduleTask;
import com.tapdata.entity.TaskHistory;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class TapdataTaskScheduler {

	private Logger logger = LogManager.getLogger(getClass());

	private final static String PKG_PATH = "io.tapdata.task";

	private ClientMongoOperator clientMongoOperator;

	private String agent_id;

	private TaskScheduler taskScheduler;

	private Map<String, TaskExecuteInfo> schedulingTasks;

	private Map<String, Object> tapdataBulitInData;

	private SettingService settingService;

	public TapdataTaskScheduler(ClientMongoOperator clientMongoOperator, String mongoURI, SettingService settingService, String mongoConnParams) {
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(20);
		taskScheduler = new ConcurrentTaskScheduler(executorService);

		String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
		agent_id = AgentUtil.readAgentId(tapdataWorkDir);

		this.clientMongoOperator = clientMongoOperator;

		schedulingTasks = new ConcurrentHashMap<>();

		Map<String, Object> tapdataBuiltInData = new HashMap<>();
		tapdataBuiltInData.put(TaskContext.META_DB_URI_ARG, mongoURI);
		tapdataBuiltInData.put(TaskContext.META_DB_PARAMS, mongoConnParams);
		this.tapdataBulitInData = MapUtils.unmodifiableMap(tapdataBuiltInData);
		this.settingService = settingService;

	}

	public void start() {
		taskScheduler.schedule(() -> taskListen(), new PeriodicTrigger(1000));
	}

	public void taskListen() {

		long currentMills = System.currentTimeMillis();
		Query waitingScheduledQuery = new Query();
		waitingScheduledQuery.addCriteria(new Criteria().orOperator(where("status").is("waiting"),
				where("status").is("scheduling").orOperator(where("ping_time").lte(Double.valueOf(currentMills - 60000)), where("ping_time").exists(false))
		));

		Update schedulingUpdate = new Update();
		schedulingUpdate.set("status", "scheduling");
		schedulingUpdate.set("agent_id", agent_id);
		schedulingUpdate.set("ping_time", currentMills);
		schedulingUpdate.currentDate("last_updated");

		ScheduleTask scheduleTask = clientMongoOperator.findAndModify(waitingScheduledQuery, schedulingUpdate, ScheduleTask.class, ConnectorConstant.SCHEDULE_TASK_COLLECTION, true);

		if (scheduleTask != null) {
			try {
				long period = scheduleTask.getPeriod();

				Task task = getTask(scheduleTask.getTask_type());
				if (task != null) {
					TaskContext context = new TaskContext(scheduleTask, tapdataBulitInData, clientMongoOperator, settingService);
					task.initialize(context);
					if (period > 0) {

						Trigger trigger = new PeriodicTrigger(period);
						ScheduledFuture<?> scheduledFuture = taskScheduler.schedule(() -> executeTask(task, scheduleTask), trigger);
						schedulingTasks.put(scheduleTask.getId(),
								new TaskExecuteInfo(scheduledFuture, scheduleTask)
						);
					} else if (StringUtils.isNotBlank(scheduleTask.getCron_expression())) {

						Trigger trigger = new CronTrigger(scheduleTask.getCron_expression());
						ScheduledFuture<?> scheduledFuture = taskScheduler.schedule(() -> executeTask(task, scheduleTask), trigger);
						schedulingTasks.put(scheduleTask.getId(),
								new TaskExecuteInfo(scheduledFuture, scheduleTask)
						);
					} else {

						ScheduledFuture<?> scheduledFuture = taskScheduler.schedule(() ->
								executeTask(task, scheduleTask), new Date()
						);
						schedulingTasks.put(scheduleTask.getId(), new TaskExecuteInfo(scheduledFuture, scheduleTask));
					}
				} else {
//					logger.warn("Cannot support this task type {}, task id {} task name {} .", scheduleTask.getTask_type(), scheduleTask.getId(), scheduleTask.getTask_name());
				}
			} catch (Exception e) {
				logger.error("Schedule task {} failed {}", scheduleTask.getTask_name(), e.getMessage(), e);
			}
		}

		pingTask();

		stopTask();
	}

	private void executeTask(Task task, ScheduleTask scheduleTask) {
		long startTS = System.currentTimeMillis();
		try {
			task.execute(taskResult -> {

				long endTS = System.currentTimeMillis();
				long durationTS = endTS - startTS;

				recordTaskHistory(scheduleTask.getTask_name(), scheduleTask.getId(), durationTS, taskResult.getTaskResult(), taskResult.getTaskResultCode(), startTS);
			});
		} catch (Exception e) {
			String message = e.getMessage();

			long endTS = System.currentTimeMillis();
			long durationTS = endTS - startTS;

			logger.error("Process task {} result failed {}", scheduleTask, e.getMessage(), e);

			recordTaskHistory(scheduleTask.getTask_name(), scheduleTask.getId(), durationTS, message, 201, startTS);

		}

	}

	private void recordTaskHistory(String taskName, String taskId, long durationTS, Object taskResult, int taskResultCode, long startTS) {

		TaskHistory taskHistory = new TaskHistory();
		taskHistory.setAgent_id(agent_id);
		taskHistory.setTask_start_time(new Date(startTS));
		taskHistory.setTask_name(taskName);
		taskHistory.setTask_id(taskId);

		taskHistory.setTask_result(taskResult);
		taskHistory.setTask_result_code(taskResultCode);

		taskHistory.setTask_duration(durationTS);

		clientMongoOperator.insertOne(taskHistory, ConnectorConstant.TASK_HISTORY_COLLECTION);
	}

	private Task getTask(String task_type) {

		Task task = null;
		try {
			Set<BeanDefinition> taskDefinitions = PkgAnnoUtil.getBeanSetWithAnno(Arrays.asList(PKG_PATH), Arrays.asList(TaskType.class));
			for (BeanDefinition beanDefinition : taskDefinitions) {
				if (null == beanDefinition || null == beanDefinition.getBeanClassName()) continue;
				Class<Task> aClass = (Class<Task>) Class.forName(beanDefinition.getBeanClassName());
				TaskType annotation = aClass.getAnnotation(TaskType.class);
				if (null == annotation || null == annotation.type()) continue;
				if (task_type.equals(annotation.type())) {
					task = aClass.newInstance();
				}
			}
		} catch (Exception e) {
			logger.warn("Find task type {} executor fail {} ", task_type, e.getMessage());
		}
		return task;
	}

	private void pingTask() {
		for (Iterator<Map.Entry<String, TaskExecuteInfo>> it = schedulingTasks.entrySet().iterator(); it.hasNext(); ) {
			Map.Entry<String, TaskExecuteInfo> entry = it.next();
			String task_id = entry.getKey();
			TaskExecuteInfo taskExecuteInfo = entry.getValue();
			try {
				ScheduledFuture scheduledFuture = taskExecuteInfo.getScheduledFuture();
				if (!scheduledFuture.isDone()) {
					Query query = new Query(where("_id").is(task_id).and("agent_id").is(agent_id));
					Update update = new Update();
					update.set("ping_time", System.currentTimeMillis());
					update.currentDate("last_updated");
					UpdateResult updateResult = clientMongoOperator.update(query, update, ConnectorConstant.SCHEDULE_TASK_COLLECTION);
					if (updateResult != null && updateResult.getModifiedCount() <= 0) {
						stopTaskFromScheduler(task_id);
					}
				} else {
					String taskId = taskExecuteInfo.getScheduleTask().getId();
					Query watingPasuedQuery = new Query();
					watingPasuedQuery.addCriteria(where("agent_id").is(agent_id).and("id").is(taskId));
					List<ScheduleTask> scheduleTasks = clientMongoOperator.find(watingPasuedQuery, ConnectorConstant.SCHEDULE_TASK_COLLECTION, ScheduleTask.class);
					if (CollectionUtils.isNotEmpty(scheduleTasks)) {
						ScheduleTask scheduleTask = scheduleTasks.get(0);
						if (need2Paused(scheduleTask)) {
							Query query = new Query(where("_id").is(task_id));
							Update update = new Update().set("status", "paused");
							clientMongoOperator.update(query, update, ConnectorConstant.SCHEDULE_TASK_COLLECTION);
						}
					}

					it.remove();
				}
			} catch (Exception e) {
				logger.error("Task scheduler ping task {} failed {}", taskExecuteInfo.getScheduleTask().getTask_name(), e.getMessage(), e);
			}
		}
	}

	private boolean need2Paused(ScheduleTask scheduleTask) {
		return "stopping".equals(scheduleTask.getStatus()) ||
				("scheduling".equals(scheduleTask.getStatus()) && scheduleTask.getPeriod() <= 0);
	}

	private void stopTask() {
		if (MapUtils.isNotEmpty(schedulingTasks)) {
			long currentMills = System.currentTimeMillis();
			Query watingPasuedQuery = new Query();
			watingPasuedQuery.addCriteria(where("status").is("stopping").orOperator(where("agent_id").is(agent_id), where("ping_time").lte(Double.valueOf(currentMills - 60000))));

			List<ScheduleTask> scheduleTasks = clientMongoOperator.find(watingPasuedQuery, ConnectorConstant.SCHEDULE_TASK_COLLECTION, ScheduleTask.class);
			if (CollectionUtils.isNotEmpty(scheduleTasks)) {
				for (ScheduleTask task : scheduleTasks) {
					String task_id = task.getId();
					if (schedulingTasks.containsKey(task_id)) {
						stopTaskFromScheduler(task_id);
					} else {
						Query query = new Query(where("_id").is(task_id));
						Update update = new Update().set("status", "paused");
						clientMongoOperator.update(query, update, ConnectorConstant.SCHEDULE_TASK_COLLECTION);
					}
				}
			}
		}
	}

	private void stopTaskFromScheduler(String task_id) {
		TaskExecuteInfo taskExecuteInfo = schedulingTasks.get(task_id);
		ScheduledFuture scheduledFuture = taskExecuteInfo.getScheduledFuture();
		scheduledFuture.cancel(false);
		taskExecuteInfo.getScheduleTask().setStatus("paused");
	}

	public static void main(String[] args) throws InterruptedException {
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(20);
		ConcurrentTaskScheduler taskScheduler = new ConcurrentTaskScheduler(executorService);
//        PeriodicTrigger trigger = new PeriodicTrigger(5000);
		Future<?> schedule = taskScheduler.submit(() -> {
			System.out.println("Already done.");
		});

		while (true) {
			System.out.println(schedule.isDone());
			Thread.sleep(1000);
		}
	}

}
