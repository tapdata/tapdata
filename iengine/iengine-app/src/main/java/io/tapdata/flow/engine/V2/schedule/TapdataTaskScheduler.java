package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import io.tapdata.utils.AppType;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskOpRespDto;
import com.tapdata.tm.sdk.available.TmStatusService;
import io.tapdata.common.SettingService;
import io.tapdata.dao.MessageDao;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.RestDoNotRetryException;
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.flow.engine.V2.common.FixScheduleTaskConfig;
import io.tapdata.flow.engine.V2.common.ScheduleTaskConfig;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskClient;
import io.tapdata.flow.engine.V2.task.operation.StartTaskOperation;
import io.tapdata.flow.engine.V2.task.operation.StopTaskOperation;
import io.tapdata.flow.engine.V2.task.operation.TaskOperation;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryFactory;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryService;
import io.tapdata.flow.engine.V2.util.SingleLockWithKey;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.core.api.PDKIntegration;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;


/**
 * @author jackin
 */
@Component
@DependsOn("connectorManager")
public class TapdataTaskScheduler implements MemoryFetcher {
	public static final String TAG = TapdataTaskScheduler.class.getSimpleName();
	private Logger logger = LogManager.getLogger(TapdataTaskScheduler.class);
	private Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();
	/**
	 * Tasks that need to be stopped internally
	 */
	private final Map<String, TaskClient<TaskDto>> internalStopTaskClientMap = new ConcurrentHashMap<>();
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
	@Qualifier("taskControlScheduler")
	@Autowired
	private TaskScheduler taskScheduler;
	private final LinkedBlockingQueue<TaskOperation> taskOperationsQueue = new LinkedBlockingQueue<>(100);
	private final ExecutorService taskOperationThreadPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() + 1, Runtime.getRuntime().availableProcessors() + 1,
			0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
	private final Map<String, ScheduleTaskConfig> scheduleTaskConfigs = new ConcurrentHashMap<>();
	private final Map<String, ScheduledFuture<?>> scheduledFutureMap = new ConcurrentHashMap<>();

	public final static String SCHEDULE_START_TASK_NAME = "scheduleStartTask";
	public final static String SCHEDULE_STOP_TASK_NAME = "scheduleStopTask";
	private static final SingleLockWithKey taskLock = new SingleLockWithKey();
	private static final Map<String, Long> taskRetryTimeMap = new ConcurrentHashMap<>();
	private static final ScheduledExecutorService taskResetRetryServiceScheduledThreadPool = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Task-Reset-Retry-Service-Scheduled-Runner"));
	//private ThreadPoolExecutorEx threadPoolExecutorEx;

	@Bean(name = "taskControlScheduler")
	public TaskScheduler taskControlScheduler() {
		return new ThreadPoolTaskScheduler();
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
					Criteria rescheduleCriteria = where("_id").is(taskId).andOperator(where("agentId").ne(instanceNo), where("agentId").ne(null));

					Query query = new Query(rescheduleCriteria);
					query.fields().include("id").include("status");
					final List<TaskDto> subTaskDtos = clientMongoOperator.find(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
					if (CollectionUtils.isNotEmpty(subTaskDtos)) {
						internalStopTaskClientMap.put(taskId, subTaskDtoTaskClient);
						removeTask(taskId);
					} else {
						TmStatusService.setAllowReport(taskId);
					}
				}
			} catch (Exception e) {
				logger.error("Scan reschedule task failed {}", e.getMessage(), e);
			}
		});
		if (AppType.currentType().isCloud()) {
			taskScheduler.scheduleAtFixedRate(this::internalStopTask, Duration.ofSeconds(10));
		}
		taskOperationThreadPool.submit(() -> {
			Thread.currentThread().setName("Task-Operation-Consumer");
			while (true) {
				try {
					TaskOperation taskOperation = taskOperationsQueue.poll(1L, TimeUnit.SECONDS);
					if (null == taskOperation) continue;
					handleTaskOperation(taskOperation);
				} catch (InterruptedException e) {
					break;
				} catch (Throwable throwable) {
					logger.error("Poll task operation queue failed: " + throwable.getMessage(), throwable);
				}
			}
		});
		initScheduleTask();
		taskResetRetryServiceScheduledThreadPool.scheduleWithFixedDelay(this::resetTaskRetryServiceIfNeed, 1L, 1L, TimeUnit.MINUTES);
		PDKIntegration.registerMemoryFetcher("taskScheduler", this);
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

	protected Runnable getHandleTaskOperationRunnable(TaskOperation taskOperation) {
		return () -> {
			String taskId;
			try {
				if (taskOperation instanceof StartTaskOperation) {
					StartTaskOperation startTaskOperation = (StartTaskOperation) taskOperation;
					Thread.currentThread().setName(String.format("Start-Task-Operation-Handler-%s[%s]", startTaskOperation.getTaskDto().getName(), startTaskOperation.getTaskDto().getId()));
					taskId = startTaskOperation.getTaskDto().getId().toHexString();
					TaskDto taskDto = startTaskOperation.getTaskDto();
					if (!taskLock.tryRun(taskId, ()-> startTask(taskDto), 1L, TimeUnit.SECONDS)) {
						logger.warn("Start task {} failed because of task lock, will ignored", taskDto.getName());
						ObsLoggerFactory.getInstance().getObsLogger(taskDto).warn("Start task failed because of task lock, will ignored");
						ObsLoggerFactory.getInstance().removeTaskLoggerMarkRemove(taskDto);
					}
				} else if (taskOperation instanceof StopTaskOperation) {
					StopTaskOperation stopTaskOperation = (StopTaskOperation) taskOperation;
					Thread.currentThread().setName(String.format("Stop-Task-Operation-Handler-%s", stopTaskOperation.getTaskId()));
					taskId = stopTaskOperation.getTaskId();
					if (!taskLock.tryRun(taskId, () -> stopTask(taskId), 1L, TimeUnit.SECONDS)) {
						logger.warn("Stop task {} failed because of task lock, will retry later", taskId);
						Optional.ofNullable(ObsLoggerFactory.getInstance().getObsLogger(taskId))
								.ifPresent(log -> log.warn("Stop task failed because of task lock, will ignored"));
					}
				}
				logger.info("Handled task operation: {}", taskOperation);
			} catch (Exception e) {
				if (TmUnavailableException.isInstance(e)) {
					logger.warn("Handle task operation failed because TM unavailable: {}", e.getMessage());
				} else {
					logger.error("Handle task operation error", e);
				}
			}
		};
	}

	protected void handleTaskOperation(TaskOperation taskOperation) {
		Runnable runnable = getHandleTaskOperationRunnable(taskOperation);
		taskOperationThreadPool.submit(runnable);
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
		clientMongoOperator.postOne(null, ConnectorConstant.TASK_COLLECTION + "/stopTaskByAgentId/" + instanceNo, Object.class);
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
		if (!AppType.currentType().isCloud()) return;
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

	protected void startTask(TaskDto taskDto) {
		ObsLoggerFactory.getInstance().removeTaskLoggerClearMark(taskDto);
		final String taskId = taskDto.getId().toHexString();
		AtomicBoolean isReturn = new AtomicBoolean(false);
		taskClientMap.computeIfPresent(taskId, (id, taskClient)->{
			if (taskClientMap.containsKey(taskId)) {
				String status = taskClient.getStatus();
				logger.info("The [task {}, id {}, status {}] is being executed, ignore the scheduling", taskDto.getName(), taskId, status);
				Optional.ofNullable(ObsLoggerFactory.getInstance().getObsLogger(taskId))
						.ifPresent(log -> log.info("This task is already running"));
				try {
					clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/running", taskId, TaskDto.class);
				} catch (Exception e) {
					if (e instanceof RestDoNotRetryException && "Transition.Not.Supported".equals(((RestDoNotRetryException) e).getCode())) {
						// ignored Transition.Not.Supported error
					} else {
						throw e;
					}
				}
				isReturn.compareAndSet(false, true);
			}
			return taskClient;
		});
		if (isReturn.get()) {
			return;
		}
		try {
			logger.info("The task to be scheduled is found, task name {}, task id {}", taskDto.getName(), taskId);
			TmStatusService.addNewTask(taskId);
			clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/running", taskId, TaskDto.class);
			final TaskClient<TaskDto> subTaskDtoTaskClient = hazelcastTaskService.startTask(taskDto);
			taskClientMap.put(subTaskDtoTaskClient.getTask().getId().toHexString(), subTaskDtoTaskClient);
		} catch (Throwable e) {
			if (TmUnavailableException.isInstance(e)) {
				logger.warn("Start task {} failed because TM unavailable: {}", taskDto.getName(), e.getMessage());
			} else {
				logger.error("Start task {} failed {}", taskDto.getName(), e.getMessage(), e);
				CompletableFuture.runAsync(() -> clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/runError", taskId, TaskDto.class)).join();
			}
			if (ObsLoggerFactory.getInstance().getObsLogger(taskDto) != null) {
				ObsLoggerFactory.getInstance().getObsLogger(taskDto).error( "Start task failed: " + e.getMessage(), e);
			}
			ObsLoggerFactory.getInstance().removeTaskLoggerMarkRemove(taskDto);
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

			if (AppType.currentType().isCloud()) return;

			List<TaskDto> timeoutStoppingTasks = findStoppingTasks();
			for (TaskDto timeoutStoppingTask : timeoutStoppingTasks) {
				final String taskId = timeoutStoppingTask.getId().toHexString();
				clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/stopped", taskId, TaskDto.class);
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
				if (!taskClient.isRunning()) {
					taskLock.tryRun(taskId, () -> {
						StopTaskResource stopTaskResource = null;
						TerminalMode terminalMode = taskClient.getTerminalMode();
						if (TerminalMode.STOP_GRACEFUL == terminalMode) {
							stopTaskResource = StopTaskResource.STOPPED;
						} else if (TerminalMode.COMPLETE == terminalMode) {
                            // 确保任务启动周期大于 5 秒，让指标上报一次
                            if (System.currentTimeMillis() - taskClient.getCreateTime() < 5000) return;

                            stopTaskResource = StopTaskResource.COMPLETE;
                        } else if (TerminalMode.INTERNAL_STOP == terminalMode) {
							if (taskClient.stop()) {
								clearTaskCacheAfterStopped(taskClient);
								clearTaskRetryCache(taskId);
							}
						} else {
							logger.warn("Task status to error: {}", terminalMode);
							TaskRetryService taskRetryService = TaskRetryFactory.getInstance().getTaskRetryService(taskId).orElse(null);
							if (null != taskRetryService) {
								TaskRetryService.TaskRetryResult taskRetryResult = taskRetryService.canTaskRetry();
								if (taskRetryResult.isCanRetry()) {
									boolean stop = taskClient.stop();
									if (stop) {
										clearTaskCacheAfterStopped(taskClient);
										TaskDto taskDto = clientMongoOperator.findOne(Query.query(where("_id").is(taskId)), ConnectorConstant.TASK_COLLECTION, TaskDto.class);
										ObsLoggerFactory.getInstance().getObsLogger(taskClient.getTask()).info("Resume task[{}]", taskClient.getTask().getName());
										long retryStartTime = System.currentTimeMillis();
										sendStartTask(taskDto);
										taskRetryTimeMap.put(taskId, retryStartTime);
									}
								} else {
									stopTaskResource = StopTaskResource.RUN_ERROR;
									if (StringUtils.isNotBlank(taskRetryResult.getCantRetryReason())) {
										ObsLoggerFactory.getInstance().getObsLogger(taskClient.getTask())
												.info("Task [{}] cannot retry, reason: {}", taskClient.getTask().getName(), taskRetryResult.getCantRetryReason());
									}
								}
							} else {
								stopTaskResource = StopTaskResource.RUN_ERROR;
							}
						}
						if (null != stopTaskResource) {
							taskClient.getTask().setSnapShotInterrupt(true);
							stopTaskAndClear(taskClient, stopTaskResource, taskId);
						}
					}, 1L, TimeUnit.SECONDS);
				}
			}
		} catch (Exception e) {
			logger.error("Scan force stopping data flow failed {}", e.getMessage(), e);
		}
	}




	private void internalStopTask() {
		Thread.currentThread().setName(String.format(ConnectorConstant.CLOUD_INTERNAL_STOP_TASK_THREAD, instanceNo));
		try {
			Iterator<Map.Entry<String, TaskClient<TaskDto>>> iterator = internalStopTaskClientMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<String, TaskClient<TaskDto>> entry = iterator.next();
				TaskClient<TaskDto> taskClient = entry.getValue();
				final String taskId = taskClient.getTask().getId().toHexString();
				final boolean stop = taskClient.stop();
				if (stop) {
					try {
						destroyCache(taskClient);
						logger.info(String.format("Destroy memory task client cache succeed, task: %s[%s]", taskClient.getTask().getName(), taskId));
					} catch (Exception e) {
						throw new RuntimeException(String.format("Destroy memory task client cache failed, task: %s[%s]", taskClient.getTask().getName(), taskId), e);
					}
					iterator.remove();
				}
			}
		} catch (Exception e) {
			logger.error("Scan internal stopping data flow failed {}", e.getMessage(), e);
		}
	}

	private static void clearTaskRetryCache(String taskId) {
		TaskRetryFactory.getInstance().removeTaskRetryService(taskId);
		taskRetryTimeMap.remove(taskId);
	}

	protected void resetTaskRetryServiceIfNeed() {
		try {
			Iterator<Map.Entry<String, Long>> iterator = taskRetryTimeMap.entrySet().iterator();
			while (iterator.hasNext()){
				Map.Entry<String, Long> entry = iterator.next();
				String taskId = entry.getKey();
				Long taskRetryStartTimeMs = entry.getValue();
				if (StringUtils.isBlank(taskId) || null == taskRetryStartTimeMs) {
					continue;
				}
				TaskRetryService taskRetryService = TaskRetryFactory.getInstance().getTaskRetryService(taskId).orElse(null);
				if (null == taskRetryService) {
					continue;
				}
				long taskRetrySucceedTimeMs = TimeUnit.HOURS.toMillis(1L);
				long currentTimeMillis = System.currentTimeMillis();
				if (currentTimeMillis - taskRetryStartTimeMs >= taskRetrySucceedTimeMs) {
					taskRetryService.reset();
					ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskId);
					if (null != obsLogger) {
						TaskClient<TaskDto> taskDtoTaskClient = taskClientMap.get(taskId);
						if (null != taskDtoTaskClient) {
							obsLogger.trace(String.format("Reset task [%s] retry time", taskDtoTaskClient.getTask().getName()));
						}
					}
					iterator.remove();
				}
			}
		} catch (Throwable ignored) {
		}
	}

	private void destroyCache(TaskClient<TaskDto> taskClient) {
		String cacheName = taskClient.getCacheName();
		if (StringUtils.isNotEmpty(cacheName)) {
			messageDao.updateCacheStatus(cacheName, taskClient.getStatus());
			messageDao.destroyCache(taskClient.getTask(), cacheName);
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
	protected List<TaskDto> findStoppingTasks() {
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
		return CollectionUtils.isNotEmpty(subTaskDtos) ? subTaskDtos.get(0) : null;
	}

	public boolean stopTaskCallAssignApi(TaskClient<TaskDto> taskClient, StopTaskResource stopTaskResource) {
		if (taskClient == null || taskClient.getTask() == null || StringUtils.isBlank(taskClient.getTask().getId().toHexString())) {
			return true;
		}
		final boolean stop = taskClient.stop();
		if (stop) {
			final TaskDto task = taskClient.getTask();
			final String taskName = task.getName();
			final String taskId = task.getId().toHexString();

			Optional.ofNullable(ObsLoggerFactory.getInstance().getObsLogger(taskId)).ifPresent(log -> log.info("Task stopped."));

			String resource = ConnectorConstant.TASK_COLLECTION + "/" + stopTaskResource.getResource();
			try {
				logger.info("Call {} api to modify task [{}] status", resource, taskName);
				AtomicBoolean success = new AtomicBoolean(true);
				CompletableFuture.runAsync(() -> {
					TaskOpRespDto taskOpRespDto = clientMongoOperator.updateById(new Update(), resource, taskId, TaskOpRespDto.class);
					if (CollectionUtils.isEmpty(taskOpRespDto.getSuccessIds())) {
						success.set(false);
					}
				}).join();
				return success.get();
			} catch (Exception e) {
				if (StringUtils.isNotBlank(e.getMessage()) && e.getMessage().contains("Transition.Not.Supported")) {
					// 违反TM状态机，不再进行修改任务状态的重试
					logger.warn("Call api to stop task status to {} failed, will set task to error, message: {}", resource, e.getMessage(), e);
					CompletableFuture.runAsync(() -> clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/" + StopTaskResource.RUN_ERROR.getResource(), taskId, TaskDto.class)).join();
					return true;
				} else {
					logger.warn("Call stop task api failed, api uri: {}, task: {}[{}]", resource, taskName, taskId, e);
				}
			}
		}
		return false;
	}

	private void clearTaskCacheAfterStopped(TaskClient<TaskDto> taskClient) {
		if (null == taskClient) {
			return;
		}
		String taskId = taskClient.getTask().getId().toHexString();
		ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskClient.getTask());
		try {
			removeTask(taskId);
			obsLogger.trace(String.format("Remove memory task client succeed, task: %s[%s]",
					taskClient.getTask().getName(), taskClient.getTask().getId()));
		} catch (Exception e) {
			throw new RuntimeException(String.format("Remove memory task client failed, task: %s[%s]",
					taskClient.getTask().getName(), taskClient.getTask().getId()), e);
		}
		try {
			destroyCache(taskClient);
			obsLogger.trace(String.format("Destroy memory task client cache succeed, task: %s[%s]", taskClient.getTask().getName(), taskClient.getTask().getId()));
		} catch (Exception e) {
			throw new RuntimeException(String.format("Destroy memory task client cache failed, task: %s[%s]", taskClient.getTask().getName(), taskClient.getTask().getId()), e);
		}
	}

	private void addAgentIdUpdate(Update update) {
		update.set("agentId", instanceNo);
	}

	private long getJobHeartTimeout() {
		return settingService.getLong("jobHeartTimeout", 60000L);
	}

	protected void stopTask(String taskId) {
		TaskClient<TaskDto> taskDtoTaskClient = taskClientMap.get(taskId);
		if (null == taskDtoTaskClient) {
			try {
				clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/stopped", taskId, TaskDto.class);
				Optional.ofNullable(ObsLoggerFactory.getInstance().getObsLogger(taskId)).ifPresent(log -> log.info("This task already stopped."));
			} catch (Exception e) {
				logger.warn(e.getMessage(), e);
			}
			return;
		}
		taskDtoTaskClient.terminalMode(TerminalMode.STOP_GRACEFUL);
		taskDtoTaskClient.getTask().setSnapShotInterrupt(true);
		stopTaskAndClear(taskDtoTaskClient, StopTaskResource.STOPPED, taskId);
	}

	private void stopTaskAndClear(TaskClient<TaskDto> taskDtoTaskClient, StopTaskResource stopped, String taskId) {
		if (stopTaskCallAssignApi(taskDtoTaskClient, stopped)) {
			clearTaskCacheAfterStopped(taskDtoTaskClient);
			clearTaskRetryCache(taskId);
			ObsLoggerFactory.getInstance().removeTaskLoggerMarkRemove(taskDtoTaskClient.getTask());
		}
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

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create();
		DataMap taskMap = DataMap.create();
		taskClientMap.forEach((k, v) -> {
			DataMap task = DataMap.create();
			task.kv("task status", v.getTask().getStatus());
			if (v instanceof HazelcastTaskClient) {
				task.kv("jet status", ((HazelcastTaskClient) v).getJetStatus());
			}
			taskMap.kv(v.getTask().getName(), task);
		});
		dataMap.kv("task client map", taskMap);
		return dataMap;
	}

	protected enum StopTaskResource {
		STOPPED("stopped"),
		RUN_ERROR("runError"),
		COMPLETE("complete"),
		;
		private String resource;

		StopTaskResource(String resource) {
			this.resource = resource;
		}

		public String getResource() {
			return resource;
		}
	}
}
