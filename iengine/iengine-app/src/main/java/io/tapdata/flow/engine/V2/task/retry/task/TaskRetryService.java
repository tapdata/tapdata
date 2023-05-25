package io.tapdata.flow.engine.V2.task.retry.task;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.flow.engine.V2.task.retry.RetryService;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2023-03-11 17:10
 **/
public class TaskRetryService extends RetryService implements Serializable {
	private static final long serialVersionUID = -4915347734458547440L;
	private final Object lock = new Object();
	private Long startRetryTimeMs;
	private Long endRetryTimeMs;

	protected TaskRetryService(TaskRetryContext taskRetryContext) {
		super(taskRetryContext);
	}

	static TaskRetryService create(TaskRetryContext taskRetryContext) {
		return new TaskRetryService(taskRetryContext);
	}

	@Override
	public void start() {
		synchronized (this.lock) {
			if (null == startRetryTimeMs) {
				startRetryTimeMs = System.currentTimeMillis();
				endRetryTimeMs = startRetryTimeMs + ((TaskRetryContext) retryContext).getRetryDurationMs();
			}
		}
	}

	public long getMethodRetryDurationMs(long retryIntervalMs) {
		if (((TaskRetryContext) retryContext).getRetryDurationMs().compareTo(0L) <= 0) {
			return 0L;
		}
		long methodRetryDurationMs = retryIntervalMs * ((TaskRetryContext) retryContext).getMethodRetryTime();
		if (null != endRetryTimeMs) {
			long currentTimeMillis = System.currentTimeMillis();
			if (currentTimeMillis > endRetryTimeMs) {
				return 0L;
			}
			methodRetryDurationMs = Math.min(
					methodRetryDurationMs,
					endRetryTimeMs - System.currentTimeMillis()
			);
		} else {
			methodRetryDurationMs = Math.min(
					methodRetryDurationMs,
					((TaskRetryContext) retryContext).getRetryDurationMs()
			);
		}
		return Math.max(0L, methodRetryDurationMs);
	}

	public TaskRetryResult canTaskRetry() {
		if (((TaskRetryContext) retryContext).getRetryDurationMs().compareTo(0L) <= 0) {
			return new TaskRetryResult(false, "Max retry duration set to 0");
		}
		if (null == endRetryTimeMs) {
			return new TaskRetryResult(false, "Task retry service not start");
		}
		if (System.currentTimeMillis() > endRetryTimeMs) {
			return new TaskRetryResult(false, "Maximum retry time exceeded: " + ZonedDateTime.ofInstant(Instant.ofEpochMilli(endRetryTimeMs), ZoneId.systemDefault()));
		}
		TaskDto taskDto = ((TaskRetryContext) retryContext).getTaskDto();
		String taskId = taskDto.getId().toHexString();
		ClientMongoOperator clientMongoOperator = ConnectorConstant.clientMongoOperator;
		Query query = Query.query(where("_id").is(taskId));
		query.fields().include("attrs.syncProgress").include("type");
		TaskDto findTask = clientMongoOperator.findOne(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
		if (findTask == null) {
			return new TaskRetryResult(false, "Task not found, maybe deleted");
		}
		Map<String, Object> attrs = findTask.getAttrs();
		SyncTypeEnum syncType = SyncTypeEnum.get(findTask.getType());
		if (syncType.equals(SyncTypeEnum.CDC)) {
			return new TaskRetryResult(true);
		}
		if (MapUtils.isNotEmpty(attrs) && attrs.containsKey("syncProgress")) {
			// Check task sync progress
			// If all sync pipeline run into cdc, then restart task
			// If not, then cancel task
			Object syncProgress = attrs.get("syncProgress");
			if (syncProgress instanceof Map) {
				for (Map.Entry<?, ?> syncProgressEntry : ((Map<?, ?>) syncProgress).entrySet()) {
					Object key = syncProgressEntry.getKey();
					Object value = syncProgressEntry.getValue();
					if (!(key instanceof String) || !(value instanceof String)) {
						return new TaskRetryResult(false, "Sync progress key or value not string type");
					}
					try {
						SyncProgress progress = JSONUtil.json2POJO((String) value, SyncProgress.class);
						if (null == progress) {
							return new TaskRetryResult(false, "Sync progress is empty");
						}
						String streamOffset = progress.getStreamOffset();
						String syncStage = progress.getSyncStage();
						if (null == syncStage) {
							return new TaskRetryResult(false, String.format("Sync stage is null, key: %s, progress: %s", key, progress));
						}
						if (!SyncStage.CDC.name().equals(syncStage) || StringUtils.isBlank(streamOffset)) {
							return new TaskRetryResult(false, String.format("Sync stage is not CDC and stream offset is empty, key: %s, progress: %s", key, progress));
						}
					} catch (IOException e) {
						return new TaskRetryResult(false, "Check sync progress if can do task retry error: " + e.getMessage());
					}
				}
			}
		} else {
			return new TaskRetryResult(false, "Sync progress is empty");
		}
		return new TaskRetryResult(true);
	}

	@Override
	public void reset() {
		synchronized (this.lock) {
			this.startRetryTimeMs = null;
			this.endRetryTimeMs = null;
		}
	}

	public Long getStartRetryTimeMs() {
		return startRetryTimeMs;
	}

	public Long getEndRetryTimeMs() {
		return endRetryTimeMs;
	}

	public static class TaskRetryResult {
		private final boolean canRetry;
		private String cantRetryReason;

		public TaskRetryResult(boolean canRetry) {
			this.canRetry = canRetry;
		}

		public TaskRetryResult(boolean canRetry, String cantRetryReason) {
			this.canRetry = canRetry;
			this.cantRetryReason = cantRetryReason;
		}

		public boolean isCanRetry() {
			return canRetry;
		}

		public String getCantRetryReason() {
			return cantRetryReason;
		}
	}
}
