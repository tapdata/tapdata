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
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.SettingService;
import io.tapdata.dao.MessageDao;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.springframework.data.mongodb.core.query.Criteria.where;


/**
 * @author jackin
 */
@Component
@DependsOn("connectorManager")
public class TapdataTaskScheduler {

	private final static long LONG_TIME_EXECUTED_CAPACITY = 1000L;

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

	private final AppType appType = AppType.init();

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
	}

	/**
	 * 调度编排任务方法
	 */
	@Scheduled(fixedDelay = 10000L)
	public void scheduledTask() {
		Thread.currentThread().setName(String.format(ConnectorConstant.START_DATAFLOW_THREAD, instanceNo));

		try {
			Query query = new Query(
					new Criteria("agentId").is(instanceNo)
							.and(DataFlow.STATUS_FIELD).is(TaskDto.STATUS_WAIT_RUN)
			);
			query.with(Sort.by(DataFlow.PING_TIME_FIELD).ascending());
			Update update = new Update();
			update.set(DataFlow.PING_TIME_FIELD, System.currentTimeMillis());
			addAgentIdUpdate(update);

			TaskDto taskDto = clientMongoOperator.findAndModify(query, update, TaskDto.class, ConnectorConstant.TASK_COLLECTION, true);
			if (taskDto != null) {
				startTask(taskDto);
			}
		} catch (Exception e) {
			logger.error("Schedule task failed {}", e.getMessage(), e);
		}
	}

	public void startTask(TaskDto taskDto) {
		final String taskId = taskDto.getId().toHexString();
		if (taskClientMap.containsKey(taskId)) {
			TaskClient<TaskDto> taskClient = taskClientMap.get(taskId);
			if (null != taskClient) {
				logger.info("The [task {}, id {}, status {}] is being executed, ignore the scheduling.", taskDto.getName(), taskId, taskClient.getStatus());
				if (TaskDto.STATUS_RUNNING.equals(taskClient.getStatus())) {
					clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/running", taskId, TaskDto.class);
				}
			} else {
				logger.info("The [task {}, id {}] is being executed, ignore the scheduling.", taskDto.getName(), taskId);
			}
			return;
		}
		try {
			Log4jUtil.setThreadContext(taskDto);
			logger.info("The task to be scheduled is found, task name {}, task id {}.", taskDto.getName(), taskId);
			TmStatusService.addNewTask(taskId);
			clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/running", taskId, TaskDto.class);
			final TaskClient<TaskDto> subTaskDtoTaskClient = hazelcastTaskService.startTask(taskDto);
			taskClientMap.put(subTaskDtoTaskClient.getTask().getId().toHexString(), subTaskDtoTaskClient);
		} catch (Exception e) {
			logger.error("Schedule task {} failed {}", taskDto.getName(), e.getMessage(), e);
			clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/runError", taskId, TaskDto.class);
		} finally {
			ThreadContext.clearAll();
		}
	}

	/**
	 * 扫描状态为force stopping状态的编排任务，执行强制停止
	 */
	@Scheduled(fixedDelay = 10000L)
	public void forceStoppingTask() {

		try {
			for (Map.Entry<String, TaskClient<TaskDto>> entry : taskClientMap.entrySet()) {
				TaskClient<TaskDto> taskClient = entry.getValue();
				final TaskDto taskDto = taskClient.getTask();
				final TaskDto stopTask = findStopTask(taskDto.getId().toHexString());
				if (stopTask != null) {
					stopTask(taskClient);
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
			clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/runError", taskId, TaskDto.class);
			removeTask(taskId, false);
			destroyCache(taskClient);
		}
	}

	private void removeTask(String taskId, boolean stopAspect) {
		TaskClient<TaskDto> taskClient;
		if ((taskClient = taskClientMap.remove(taskId)) != null) {
			if (stopAspect && taskClient.getTask() != null) {
				AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()));
			}
		}
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
			clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/stopped", taskId, TaskDto.class);
			removeTask(taskId, true);
			destroyCache(taskClient);
		}
	}

	private void completeTask(TaskClient<TaskDto> taskClient) {
		if (taskClient == null || taskClient.getTask() == null || StringUtils.isBlank(taskClient.getTask().getId().toHexString())) {
			return;
		}
		final String taskId = taskClient.getTask().getId().toHexString();
		clientMongoOperator.updateById(new Update(), ConnectorConstant.TASK_COLLECTION + "/complete", taskId, TaskDto.class);
		removeTask(taskId, true);
		destroyCache(taskClient);
	}

	private void addAgentIdUpdate(Update update) {
		update.set("agentId", instanceNo);
	}

	private long getJobHeartTimeout() {
		return settingService.getLong("jobHeartTimeout", 60000L);
	}

	public void stopTask(String taskId) {
		TaskClient<TaskDto> taskDtoTaskClient = taskClientMap.get(taskId);
		if (null == taskDtoTaskClient) {
			return;
		}
		stopTask(taskDtoTaskClient);
	}
}
