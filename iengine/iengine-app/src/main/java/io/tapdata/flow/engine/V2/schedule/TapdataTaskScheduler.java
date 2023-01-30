package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.AppType;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.sdk.available.TmStatusService;
import io.tapdata.common.SettingService;
import io.tapdata.dao.MessageDao;
import io.tapdata.flow.engine.V2.common.FixScheduleTaskConfig;
import io.tapdata.flow.engine.V2.common.ScheduleTaskConfig;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.flow.engine.V2.task.operation.StartTaskOperation;
import io.tapdata.flow.engine.V2.task.operation.StopTaskOperation;
import io.tapdata.flow.engine.V2.task.operation.TaskOperation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;


/**
 * @author jackin
 */
@Component
@DependsOn("connectorManager")
public class TapdataTaskScheduler {
	private Logger logger = LogManager.getLogger(TapdataTaskScheduler.class);
	private Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();
	private String instanceNo;
	@Autowired
	private ClientMongoOperator clientMongoOperator;
	@Autowired
	private ConfigurationCenter configCenter;
	@Autowired
	private SettingService settingService;
	@Autowired
	private TaskService<TaskDto> hazelcastTaskService;
	@Autowired
	private MessageDao messageDao;
	@Autowired
	private TaskScheduler taskScheduler;
	private final AppType appType = AppType.init();
	private final LinkedBlockingQueue<TaskOperation> taskOperationsQueue = new LinkedBlockingQueue<>(100);
	private final ExecutorService taskOperationThreadPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() + 1, Runtime.getRuntime().availableProcessors() + 1,
			0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
	private CountDownLatch taskOpCountDown;
	private Map<String, ScheduleTaskConfig> scheduleTaskConfigs = new ConcurrentHashMap<>();
	private Map<String, ScheduledFuture<?>> scheduledFutureMap = new ConcurrentHashMap<>();

	public final static String SCHEDULE_START_TASK_NAME = "scheduleStartTask";
	public final static String SCHEDULE_STOP_TASK_NAME = "scheduleStopTask";
	private static Map<String, Object> taskLock = new ConcurrentHashMap<>();

	private Object lockTask(String taskId) {
		Object lock = taskLock.get(taskId);
		if (null == lock) {
			return taskLock.computeIfAbsent(taskId, s -> new int[0]);
		}
		return lock;
	}

	private void unlockTask(String taskId) {
		taskLock.remove(taskId);
	}

	@PostConstruct
	public void init() {
		instanceNo = (String) configCenter.getConfig(ConfigurationCenter.AGENT_ID);
		logger.info("[Task scheduler] instance no: {}", instanceNo);

		TmStatusService.registeredTmAvailableHandler(() -> {
			//Reconnect tm, do something
			/**
			 * 1、查询还在跑的任务，如果agentId不匹配，则停止
			 * 2、上报任务已停止/错误的状态信息
			 */
			try {
				for (Map.Entry<String, TaskClient<TaskDto>> entry : taskClientMap.entrySet()) {
					TaskClient<TaskDto> subTaskDtoTaskClient = entry.getValue();
					final TaskDto taskDto = subTaskDtoTaskClient.getTask();
					String taskId = taskDto.getId().toHexString();
					Criteria rescheduleCriteria = where("_id").is(taskId).andOperator(where("agentId").ne(instanceNo));

					Query query = new Query(rescheduleCriteria);
					query.fields().include("id").include("status");
					final List<TaskDto> subTaskDtos = clientMongoOperator.find(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
					if (CollectionUtil.isNotEmpty(subTaskDtos)) {
						stopTask(subTaskDtoTaskClient);
					} else {
						TmStatusService.setAllowReport(taskId);
					}
				}
			} catch (Exception e) {
				logger.error("Scan reschedule task failed {}", e.getMessage(), e);
			}
		});
		taskOperationThreadPool.submit(() -> {
			Thread.currentThread().setName("Task-Operation-Consumer");
			while (true) {
				try {
					TaskOperation taskOperation = taskOperationsQueue.poll(3L, TimeUnit.SECONDS);
					if (null == taskOperation) continue;
					if (null == taskOpCountDown) {
						taskOpCountDown = new CountDownLatch(1);
					} else {
						while (true) {
							if (taskOpCountDown.await(1L, TimeUnit.SECONDS)) {
								taskOpCountDown = new CountDownLatch(1);
								break;
							}
						}
					}
					handleTaskOperation(taskOperation);
				} catch (InterruptedException e) {
					break;
				} catch (Throwable throwable) {
					logger.error("Poll task operation queue failed: " + throwable.getMessage(), throwable);
				}
			}
		});
		initScheduleTask();
	}

	private void initScheduleTask() {
		FixScheduleTaskConfig startTaskScheduler = FixScheduleTaskConfig.create(SCHEDULE_START_TASK_NAME, 1000L);
		FixScheduleTaskConfig stopTaskScheduler = FixScheduleTaskConfig.create(SCHEDULE_STOP_TASK_NAME, 5 * 1000L);
		scheduleTaskConfigs.put(startTaskScheduler.getName(), startTaskScheduler);
		scheduleTaskConfigs.put(stopTaskScheduler.getName(), stopTaskScheduler);
	}

	public void startScheduleTask(String name) {
		if (StringUtils.isBlank(name)) return;
		if (scheduledFutureMap.containsKey(name)) return;
		ScheduleTaskConfig scheduleTaskConfig = scheduleTaskConfigs.get(name);
		if (null == scheduleTaskConfig) return;
		if (scheduleTaskConfig instanceof FixScheduleTaskConfig) {
			FixScheduleTaskConfig fixScheduleTaskConfig = (FixScheduleTaskConfig) scheduleTaskConfig;
			switch (name) {
				case SCHEDULE_START_TASK_NAME:
					scheduledFutureMap.put(name, taskScheduler.scheduleAtFixedRate(this::scheduledTask, fixScheduleTaskConfig.getFixedDelay()));
					break;
				case SCHEDULE_STOP_TASK_NAME:
					scheduledFutureMap.put(name, taskScheduler.scheduleAtFixedRate(this::forceStoppingTask, fixScheduleTaskConfig.getFixedDelay()));
					break;
				default:
					break;
			}
			logger.info("Start schedule task: " + name);
		}
	}

	public void stopScheduleTask(String name) {
		if (StringUtils.isBlank(name)) return;
		ScheduledFuture<?> scheduledFuture = scheduledFutureMap.get(name);
		if (null == scheduledFuture) return;
		scheduledFuture.cancel(false);
		scheduledFutureMap.remove(name);
		logger.info("Stop schedule task: " + name);
	}

	private void handleTaskOperation(TaskOperation taskOperation) {
		taskOperationThreadPool.submit(() -> {
			String taskId = null;
			try {
				if (taskOperation instanceof StartTaskOperation) {
					StartTaskOperation startTaskOperation = (StartTaskOperation) taskOperation;
					Thread.currentThread().setName(String.format("Start-Task-Operation-Handler-%s[%s]", startTaskOperation.getTaskDto().getName(), startTaskOperation.getTaskDto().getId()));
					taskId = startTaskOperation.getTaskDto().getId().toHexString();
					Object lock = lockTask(taskId);
					synchronized (lock) {
						taskOpCountDown.countDown();
						startTask(startTaskOperation.getTaskDto());
					}
				} else if (taskOperation instanceof StopTaskOperation) {
					StopTaskOperation stopTaskOperation = (StopTaskOperation) taskOperation;
					Thread.currentThread().setName(String.format("Stop-Task-Operation-Handler-%s", stopTaskOperation.getTaskId()));
					taskId = stopTaskOperation.getTaskId();
					Object lock = lockTask(taskId);
					synchronized (lock) {
						taskOpCountDown.countDown();
						stopTask(stopTaskOperation.getTaskId());
					}
				}
			} finally {
				if (StringUtils.isNotBlank(taskId)) {
					unlockTask(taskId);
				}
				if (taskOpCountDown.getCount() > 0) {
					taskOpCountDown.countDown();
				}
			}
			logger.info("Handled task operation: {}", taskOperation);
		});
	}

	public void sendStartTask(TaskDto taskDto) {
		taskOpEnqueue(StartTaskOperation.create().taskDto(taskDto));
		if (logger.isDebugEnabled()) {
			List<String> stackTraces = Arrays.stream(Thread.currentThread().getStackTrace()).map(StackTraceElement::toString).collect(Collectors.toList());
			logger.debug("Send start task operation: {}[{}]\n{}", taskDto.getName(), taskDto.getId().toHexString(), String.join("\n", stackTraces));
		} else if (logger.isInfoEnabled()) {
			logger.info("Send start task operation: {}[{}]", taskDto.getName(), taskDto.getId().toHexString());
		}
	}

	public void sendStopTask(String taskId) {
		taskOpEnqueue(StopTaskOperation.create().taskId(taskId));
		if (logger.isDebugEnabled()) {
			List<String> stackTraces = Arrays.stream(Thread.currentThread().getStackTrace()).map(StackTraceElement::toString).collect(Collectors.toList());
			logger.debug("Send stop task operation: {}\n{}", taskId, String.join("\n", stackTraces));
		} else if (logger.isInfoEnabled()) {
			logger.info("Send stop task operation: {}", taskId);
		}
	}

	private void taskOpEnqueue(TaskOperation taskOperation) {
		if (null == taskOperation) return;
		while (true) {
			try {
				if (taskOperationsQueue.offer(taskOperation)) {
					break;
				}
			} catch (Exception e) {
				break;
			}
		}
	}

	public void stopTaskIfNeed() {
		if (StringUtils.isBlank(instanceNo)) {
			return;
		}
		logger.info("Stop task which agent id is {} and status is {}", instanceNo, TaskDto.STATUS_STOPPING);
		clientMongoOperator.postOne(null, ConnectorConstant.TASK_COLLECTION+"/stopTaskByAgentId/"+instanceNo, Object.class);
	}

	/**
	 * 调度编排任务方法
	 */
	public void scheduledTask() {
		Thread.currentThread().setName(String.format(ConnectorConstant.START_TASK_THREAD, instanceNo));
		try {
			Query query = new Query(
					new Criteria("agentId").is(instanceNo)
							.and(DataFlow.STATUS_FIELD).is(TaskDto.STATUS_WAIT_RUN)
			);
			query.with(Sort.by(DataFlow.PING_TIME_FIELD).ascending());
			query.fields().include("_id").include("name");
			List<TaskDto> allWaitRunTasks = clientMongoOperator.find(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
			for (TaskDto waitRunTask : allWaitRunTasks) {
				logger.info("Staring task from http query: {}[{}]", waitRunTask.getName(), waitRunTask.getId());
				query = new Query(Criteria.where("id").is(waitRunTask.getId()));
				Update update = new Update();
				update.set(DataFlow.PING_TIME_FIELD, System.currentTimeMillis());
				addAgentIdUpdate(update);

				TaskDto taskDto = clientMongoOperator.findAndModify(query, update, TaskDto.class, ConnectorConstant.TASK_COLLECTION, true);
				if (taskDto != null) {
					sendStartTask(taskDto);
				}
			}
		} catch (Exception e) {
			logger.error("Schedule start task failed {}", e.getMessage(), e);
		}
	}

	/**
	 * Will run when engine start in cloud mode
	 * Run task(s) already started, find clause: status=running and agentID={@link TapdataTaskScheduler#instanceNo}
	 */
	public void runTaskIfNeedWhenEngineStart() {
		if(!appType.isCloud()) return;
		Query query = new Query(
				new Criteria("agentId").is(instanceNo)
						.and(DataFlow.STATUS_FIELD).is(TaskDto.STATUS_RUNNING)
		);
		List<TaskDto> tasks = clientMongoOperator.find(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
		if (CollectionUtils.isNotEmpty(tasks)) {
			logger.info("Found task(s) already running before engine start, will run these task(s) immediately\n  {}", tasks.stream().map(TaskDto::getName).collect(Collectors.joining("\n  ")));
			tasks.forEach(this::sendStartTask);
		}
	}

	private void startTask(TaskDto taskDto) {
		final String taskId = taskDto.getId().toHexString();
		if (taskClientMap.containsKey(taskId)) {
			TaskClient<TaskDto> taskClient = taskClientMap.get(taskId);
			if (null != taskClient) {
				logger.info("The [task {}, id {}, status {}] is being executed, ignore the scheduling", taskDto.getName(), taskId, taskClient.getStatus());
				if (TaskDto.STATUS_RUNNING.equals(taskClient.getStatus())) {
					clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/running", taskId, TaskDto.class);
				}
			} else {
				logger.info("The [task {}, id {}] is being executed, ignore the scheduling", taskDto.getName(), taskId);
			}
			return;
		}
		try {
			// todo 后续处理
//			String checkTaskCanStart = checkTaskCanStart(taskId);
//			if (StringUtils.isNotBlank(checkTaskCanStart)) {
//				logger.warn(checkTaskCanStart);
//				return;
//			}
			Log4jUtil.setThreadContext(taskDto);
			logger.info("The task to be scheduled is found, task name {}, task id {}", taskDto.getName(), taskId);
			TmStatusService.addNewTask(taskId);
			clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/running", taskId, TaskDto.class);
			final TaskClient<TaskDto> subTaskDtoTaskClient = hazelcastTaskService.startTask(taskDto);
			taskClientMap.put(subTaskDtoTaskClient.getTask().getId().toHexString(), subTaskDtoTaskClient);
		} catch (Exception e) {
			logger.error("Start task {} failed {}", taskDto.getName(), e.getMessage(), e);
			clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/runError", taskId, TaskDto.class);
		} finally {
			ThreadContext.clearAll();
		}
	}

	private String checkTaskCanStart(String taskId) {
		Query query = Query.query(where("_id").is(taskId));
		query.fields().include("status").include("_id").include("name");
		TaskDto taskDto = clientMongoOperator.findOne(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
		if (!taskDto.getStatus().equals(TaskDto.STATUS_WAIT_RUN) && !taskDto.getStatus().equals(TaskDto.STATUS_RUNNING)) {
			return String.format("Found task[%s(%s)] status is %s, will not start this task", taskDto.getName(), taskDto.getId().toHexString(), taskDto.getStatus());
		}
		return "";
	}

	/**
	 * 扫描状态为force stopping状态的编排任务，执行强制停止
	 */
	public void forceStoppingTask() {
		Thread.currentThread().setName(String.format(ConnectorConstant.STOP_TASK_THREAD, instanceNo));
		try {
			for (Map.Entry<String, TaskClient<TaskDto>> entry : taskClientMap.entrySet()) {
				TaskClient<TaskDto> taskClient = entry.getValue();
				final TaskDto taskDto = taskClient.getTask();
				final TaskDto stopTask = findStopTask(taskDto.getId().toHexString());
				if (stopTask != null) {
					sendStopTask(taskDto.getId().toHexString());
				}
			}

			if (!appType.isCloud()) {
				List<TaskDto> timeoutStoppingTasks = findStoppingTasks();
				for (TaskDto timeoutStoppingTask : timeoutStoppingTasks) {
					final String taskId = timeoutStoppingTask.getId().toHexString();
					clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/stopped", taskId, TaskDto.class);
				}
			}
		} catch (Exception e) {
			logger.error("Scan force stopping data flow failed {}", e.getMessage(), e);
		}
	}

	/**
	 * 检查出错的编排任务，执行强制停止
	 */
	@Scheduled(fixedDelay = 5000L)
	public void errorOrStopTask() {
		Thread.currentThread().setName(String.format(ConnectorConstant.INTERNAL_STOP_TASK_THREAD, instanceNo));
		try {
			for (Map.Entry<String, TaskClient<TaskDto>> entry : taskClientMap.entrySet()) {

				TaskClient<TaskDto> taskClient = entry.getValue();
				final String taskId = taskClient.getTask().getId().toHexString();
				if (TmStatusService.isNotAllowReport(taskId)) {
					continue;
				}
				final String status = taskClient.getStatus();
				if (TaskDto.STATUS_ERROR.equals(status)) {
					errorTask(taskClient);
				} else if (TaskDto.STATUS_STOP.equals(status) || TaskDto.STATUS_STOPPING.equals(status)) {
					stopTask(taskClient);
				} else if (TaskDto.STATUS_COMPLETE.equals(status)) {
					completeTask(taskClient);
				}
			}
		} catch (Exception e) {
			logger.error("Scan force stopping data flow failed {}", e.getMessage(), e);
		}
	}

	private void destroyCache(TaskClient<TaskDto> taskClient) {
		String cacheName = taskClient.getCacheName();
		if (StringUtils.isNotEmpty(cacheName)) {
			messageDao.updateCacheStatus(cacheName, taskClient.getStatus());
			messageDao.destroyCache(taskClient.getTask(), cacheName);
		}
	}

	private void errorTask(TaskClient<TaskDto> taskClient) {
		if (taskClient == null || taskClient.getTask() == null || StringUtils.isBlank(taskClient.getTask().getId().toHexString())) {
			return;
		}
		final boolean stop = taskClient.stop();
		if (stop) {
			final String taskId = taskClient.getTask().getId().toHexString();
			try {
				clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/runError", taskId, TaskDto.class);
			} catch (Exception e) {
				logger.warn(e.getMessage(), e);
			}
			removeTask(taskId);
			destroyCache(taskClient);
		}
	}

	private void removeTask(String taskId) {
		taskClientMap.remove(taskId);
		TmStatusService.removeTask(taskId);
	}

	/**
	 * 查找超时未停止的编排任务
	 *
	 * @return
	 */
	private List<TaskDto> findStoppingTasks() {
		long jobHeartTimeout = getJobHeartTimeout();
		long expiredTimeMillis = System.currentTimeMillis() - jobHeartTimeout;
		Criteria timeoutCriteria = where("status").is(TaskDto.STATUS_STOPPING)
//      .and("agentId").is(instanceNo)
				.orOperator(
						where("pingTime").lt(Double.valueOf(String.valueOf(expiredTimeMillis))),
						where("pingTime").is(null),
						where("pingTime").exists(false));

		Query query = new Query(timeoutCriteria);
		query.fields().include("id").include("status");
		return clientMongoOperator.find(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
	}

	/**
	 * 查找停止的编排任务
	 *
	 * @param taskId
	 * @return
	 */
	private TaskDto findStopTask(String taskId) {
		Criteria timeoutCriteria = where("_id").is(taskId)
				.orOperator(
						where("status").is(TaskDto.STATUS_STOPPING),
						where("status").is(TaskDto.STATUS_STOP)
				);

		Query query = new Query(timeoutCriteria);
		query.fields().include("id").include("status");
		final List<TaskDto> subTaskDtos = clientMongoOperator.find(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
		return CollectionUtil.isNotEmpty(subTaskDtos) ? subTaskDtos.get(0) : null;
	}

	public void stopTask(TaskClient<TaskDto> taskClient) {
		if (taskClient == null || taskClient.getTask() == null || StringUtils.isBlank(taskClient.getTask().getId().toHexString())) {
			return;
		}
		final boolean stop = taskClient.stop();
		if (stop) {
			final String taskId = taskClient.getTask().getId().toHexString();
			try {
				clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/stopped", taskId, TaskDto.class);
			} catch (Exception e) {
				logger.warn(e.getMessage(), e);
			}
			removeTask(taskId);
			destroyCache(taskClient);
		}
	}

	private void completeTask(TaskClient<TaskDto> taskClient) {
		if (taskClient == null || taskClient.getTask() == null || StringUtils.isBlank(taskClient.getTask().getId().toHexString())) {
			return;
		}
		boolean stop = taskClient.stop();
		if (stop) {
			final String taskId = taskClient.getTask().getId().toHexString();
			try {
				clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/complete", taskId, TaskDto.class);
			} catch (Exception e) {
				logger.warn(e.getMessage(), e);
			}
			removeTask(taskId);
			destroyCache(taskClient);
		}
	}

	private void addAgentIdUpdate(Update update) {
		update.set("agentId", instanceNo);
	}

	private long getJobHeartTimeout() {
		return settingService.getLong("jobHeartTimeout", 60000L);
	}

	private void stopTask(String taskId) {
		TaskClient<TaskDto> taskDtoTaskClient = taskClientMap.get(taskId);
		if (null == taskDtoTaskClient) {
			return;
		}
		taskDtoTaskClient.getTask().setManualStop(true);
		stopTask(taskDtoTaskClient);
	}

	public TaskClient<TaskDto> getTaskClient(String taskId) {
		if (null == taskClientMap) {
			return null;
		}
		return taskClientMap.get(taskId);
	}

	public Map<String, TaskClient<TaskDto>> getTaskClientMap() {
		return taskClientMap;
	}
}
