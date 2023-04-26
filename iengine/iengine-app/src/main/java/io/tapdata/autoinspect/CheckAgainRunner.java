package io.tapdata.autoinspect;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.compare.IQueryCompare;
import com.tapdata.tm.autoinspect.connector.IPdkConnector;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.constants.CheckAgainStatus;
import com.tapdata.tm.autoinspect.constants.ResultStatus;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.CheckAgainProgress;
import com.tapdata.tm.autoinspect.exception.CheckAgainException;
import com.tapdata.tm.commons.dag.nodes.AutoInspectNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.autoinspect.compare.AutoCompare;
import io.tapdata.autoinspect.compare.PdkQueryCompare;
import io.tapdata.autoinspect.connector.PdkConnector;
import io.tapdata.autoinspect.utils.AutoInspectNodeUtil;
import io.tapdata.common.SettingService;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/6 10:40 Create
 */
public class CheckAgainRunner implements Runnable {
	private static final Logger logger = LogManager.getLogger(AutoCompare.class);
	private static final String RESULTS_COLLECTION = AutoInspectConstants.AUTO_INSPECT_RESULTS_COLLECTION_NAME;
	private final @NonNull CheckAgainProgress progress;
	private final @NonNull ClientMongoOperator clientMongoOperator;
	private final @NonNull SettingService settingService;

	private final String taskId;
	private final AtomicLong checkCounts = new AtomicLong(0);
	private final AtomicLong fixCounts = new AtomicLong(0);

	public CheckAgainRunner(@NonNull String taskId, @NonNull CheckAgainProgress progress, @NonNull ClientMongoOperator clientMongoOperator, @NonNull SettingService settingService) {
		CheckAgainException.exNotScheduling(progress.getStatus());

		this.taskId = taskId;
		this.progress = progress;
		this.clientMongoOperator = clientMongoOperator;
		this.settingService = settingService;
	}

	@Override
	public void run() {
		try {
			Thread.currentThread().setName(String.format("th-%s-%s", AutoInspectConstants.AGAIN_MODULE_NAME, taskId));
			logger.info("Start CheckAgainRunner...");
			progress.setBeginAt(new Date());
			progress.setStatus(CheckAgainStatus.Running);
			intervalUpdateProgress(progress);

			Query queryTask = Query.query(Criteria.where("_id").is(new ObjectId(taskId)));
			TaskDto taskDto = clientMongoOperator.findOne(queryTask, ConnectorConstant.TASK_COLLECTION, TaskDto.class, null);
			if (null == taskDto) {
				throw new RuntimeException("Task not found: " + taskId);
			}
			TaskConfig taskConfig = getTaskConfig(taskDto);

			AutoInspectNode autoInspectNode = AutoInspectNodeUtil.firstAutoInspectNode(taskDto);
			if (null == autoInspectNode) {
				logger.warn("Not found AutoInspectNode: {}", taskId);
				return;
			}
			String sourceNodeId = autoInspectNode.getFromNode().getId();
			String targetNodeId = autoInspectNode.getToNode().getId();

			Map<String, Connections> connMap = AutoInspectNodeUtil.getConnectionsByIds(clientMongoOperator, new HashSet<>(Arrays.asList(
					autoInspectNode.getFromNode().getConnectionId(),
					autoInspectNode.getToNode().getConnectionId()
			)));
			Connections sourceConn = connMap.computeIfAbsent(autoInspectNode.getFromNode().getConnectionId(), s -> {
				throw new RuntimeException("create node failed because source connection not found: " + s);
			});
			DatabaseTypeEnum.DatabaseType sourceDatabaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, sourceConn.getPdkHash());
			Connections targetConn = connMap.computeIfAbsent(autoInspectNode.getToNode().getConnectionId(), s -> {
				throw new RuntimeException("create node failed because target connection not found: " + s);
			});
			DatabaseTypeEnum.DatabaseType targetDatabaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, targetConn.getPdkHash());

			intervalUpdateProgress(progress);
			try (IPdkConnector sourceConnector = new PdkConnector(clientMongoOperator, taskId, autoInspectNode.getFromNode(), getClass().getSimpleName() + "-" + sourceNodeId, sourceConn, sourceDatabaseType, this::isRunning, taskConfig.getTaskRetryConfig())) {
				try (IPdkConnector targetConnector = new PdkConnector(clientMongoOperator, taskId, autoInspectNode.getToNode(), getClass().getSimpleName() + "-" + targetNodeId, targetConn, targetDatabaseType, this::isRunning, taskConfig.getTaskRetryConfig())) {
					IQueryCompare queryCompare = new PdkQueryCompare(sourceConnector, targetConnector);
					intervalUpdateProgress(progress);

					eachResults(dto -> {
						IQueryCompare.Status status = queryCompare.queryCompare(dto);
						if (IQueryCompare.Status.Diff == status) {
							updateResult(dto);
						} else {
							fixResult(dto.getId());
							fixCounts.addAndGet(1);
						}

						progress.setCheckedCounts(checkCounts.addAndGet(1));
						intervalUpdateProgress(progress);
						return true;
					});
					progress.setStatus(CheckAgainStatus.Completed);
				}
			}
		} catch (Throwable e) {
			logger.error("Failed: {}", e.getMessage(), e);
			progress.setMsg("Failed: " + e.getMessage());
			if (e instanceof NullPointerException) {
				StackTraceElement[] stackTraces = e.getStackTrace();
				if (null != stackTraces) {
					for (StackTraceElement stackTrace : stackTraces) {
						if (stackTrace.getClassName().contains("tapdata")) {
							progress.setMsg("NPE " + stackTrace);
							break;
						}
					}
				}
			}
			progress.setStatus(CheckAgainStatus.Failed);

			// clear CheckAgain status
			Query query = Query.query(Criteria.where("taskId").is(taskId).and("checkAgainSN").is(progress.getSn()));
			Update update = Update.update("status", ResultStatus.Completed).set("checkAgainSN", AutoInspectConstants.CHECK_AGAIN_DEFAULT_SN).set("last_updated", new Date());
			UpdateResult update1 = clientMongoOperator.update(query, update, AutoInspectConstants.AUTO_INSPECT_RESULTS_COLLECTION_NAME);
			logger.warn("Failed reset: {}", JSON.toJSONString(update1));
		} finally {
			progress.setCompletedAt(new Date());
			logger.info("Exit CheckAgainRunner, check {}, fix {}, use {}ms", checkCounts.get(), fixCounts.get(), progress.useTimes());
			updateProgress(progress);
		}
	}

	private boolean isRunning() {
		return !Thread.interrupted();
	}

	private void intervalUpdateProgress(@NonNull CheckAgainProgress progress) {
		if (System.currentTimeMillis() - progress.getUpdated().getTime() < AutoInspectConstants.PROGRESS_UPDATE_INTERVAL) {
			updateProgress(progress);
		}
	}

	private void updateProgress(@NonNull CheckAgainProgress progress) {
		progress.setUpdated(new Date());
		progress.setCheckedCounts(checkCounts.get());
		progress.setFixCounts(fixCounts.get());

		Query query = Query.query(Criteria.where("_id").is(new ObjectId(taskId)));
		Update update = Update.update(AutoInspectConstants.CHECK_AGAIN_PROGRESS_PATH, progress);
		clientMongoOperator.update(query, update, ConnectorConstant.TASK_COLLECTION);
	}

	private void eachResults(Function<TaskAutoInspectResultDto, Boolean> callFn) {
		// 分批加载，对比过的数据会清理 checkAgainSN 所以不用 skip
		Query query = Query.query(Criteria
				.where("taskId").is(taskId)
				.and("checkAgainSN").is(progress.getSn())
		).limit(50); // .with(Sort.by("originalTableName"))

		List<TaskAutoInspectResultDto> results;
		while (!Thread.interrupted()) {
			results = clientMongoOperator.find(query, RESULTS_COLLECTION, TaskAutoInspectResultDto.class);
			if (null == results || results.isEmpty()) return;

			for (TaskAutoInspectResultDto dto : results) {
				if (!callFn.apply(dto)) {
					return;
				}
			}
		}
	}

	private void updateResult(@NonNull TaskAutoInspectResultDto dto) {
		dto.setCheckAgainSN(AutoInspectConstants.CHECK_AGAIN_DEFAULT_SN);
		dto.setStatus(ResultStatus.Completed);
		dto.setLastUpdAt(new Date());
		//bug: upsert api can not save most properties
		clientMongoOperator.insertOne(dto, RESULTS_COLLECTION);
	}

	private void fixResult(ObjectId id) {
		clientMongoOperator.delete(Query.query(Criteria.where("_id").is(id)), RESULTS_COLLECTION);
	}

	private TaskConfig getTaskConfig(TaskDto taskDto) {
		long retryIntervalSecond = settingService.getLong("retry_interval_second", 60L);
		long maxRetryTimeMinute = settingService.getLong("max_retry_time_minute", 60L);
		long maxRetryTimeSecond = maxRetryTimeMinute * 60;
		TaskRetryConfig taskRetryConfig = TaskRetryConfig.create()
				.retryIntervalSecond(retryIntervalSecond)
				.maxRetryTimeSecond(maxRetryTimeSecond);
		return TaskConfig.create().taskDto(taskDto).taskRetryConfig(taskRetryConfig);
	}
}
