package io.tapdata.flow.engine.V2.progress;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.ReflectUtil;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.progress.BatchOperationDto;
import com.tapdata.tm.commons.task.dto.progress.TaskSnapshotProgress;
import io.tapdata.Source;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author samuel
 * @Description
 * @create 2022-03-11 19:24
 **/
public class SnapshotProgressManager implements Closeable {
	private static final String TAG = SnapshotProgressManager.class.getSimpleName();
	private static final int BATCH_SIZE = 50;
	private static final int ASYNC_THRESHOLD = BATCH_SIZE * 2;
	private static final long INTERVAL = 5000L;

	private Logger logger = LogManager.getLogger(SnapshotProgressManager.class);

	private TaskDto taskDto;
	private ClientMongoOperator clientMongoOperator;
	private Map<String, List<TaskSnapshotProgress>> snapshotEdgeProgressMap;
	private Map<String, List<TaskSnapshotProgress>> incrementEdgeProgressMap;
	private ExecutorService progressThreadPool;
	private AtomicBoolean running;
	private ScheduledExecutorService flushEdgeSnapshotProgressThreadPool;
	private ScheduledExecutorService flushSubTaskSnapshotProgressThreadPool;
	private Source source;
	private Map<String, Connections> connectionsMap;
	private String currentTableName;
	private Lock lock;
	/**
	 * pdk source node
	 */
	private ConnectorNode connectorNode;
	private TapTableMap<String, TapTable> tapTableMap;

	public SnapshotProgressManager(TaskDto taskDto, ClientMongoOperator clientMongoOperator) {
		this.taskDto = taskDto;
		this.clientMongoOperator = clientMongoOperator;
	}

	public SnapshotProgressManager(TaskDto taskDto, ClientMongoOperator clientMongoOperator,
								   Source source) {
		this.taskDto = taskDto;
		this.clientMongoOperator = clientMongoOperator;
		this.source = source;
	}

	public SnapshotProgressManager(TaskDto taskDto, ClientMongoOperator clientMongoOperator,
								   ConnectorNode connectorNode, TapTableMap<String, TapTable> tapTableMap) {
		this.taskDto = taskDto;
		this.clientMongoOperator = clientMongoOperator;
		this.connectorNode = connectorNode;
		this.tapTableMap = tapTableMap;
	}

	public void startStatsSnapshotEdgeProgress(Node<?> srcNode) {
		this.running = new AtomicBoolean(true);
		this.snapshotEdgeProgressMap = new ConcurrentHashMap<>();
		this.incrementEdgeProgressMap = new ConcurrentHashMap<>();
		this.lock = new ReentrantLock();
		generateEdgeSnapshotProgresses(srcNode);
		if (snapshotEdgeProgressMap.size() >= ASYNC_THRESHOLD) {
			// 源表总数量大于阙值，则用异步的方式进行计数
			logger.info("Start to asynchronously count the number of rows in the source table(s)");
			this.progressThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
			CompletableFuture.runAsync(() -> {
						Thread.currentThread().setName("Init-Snapshot-Edge-Progress-" + taskDto.getName() + "(" + taskDto.getId().toHexString() + ")"
								+ "-" + srcNode.getId());
						writeEdgeSnapshotProgresses();
						countAndUpdateEdgeSnapshotProgresses();
					}, progressThreadPool)
					.whenComplete((v, e) -> {
						if (null != e) {
							logger.warn("Init edge snapshot progress failed: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
						} else {
							logger.info("Init edge snapshot progress completed: " + srcNode.getName() + "(" + srcNode.getId() + ")");
						}
						ExecutorUtil.shutdown(this.progressThreadPool, 10L, TimeUnit.SECONDS);
					});
		} else {
			try {
				logger.info("Start counting the number of rows in the source table(s)");
				writeEdgeSnapshotProgresses();
				countAndUpdateEdgeSnapshotProgresses();
				logger.info("Init edge snapshot progress completed: " + srcNode.getName() + "(" + srcNode.getId() + ")");
			} catch (Exception e) {
				logger.warn("Init edge snapshot progress failed: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			}
		}
		startAutoFlushEdgeProgresses(srcNode);
	}

	public void startStatsSubTaskSnapshotProgress() {
		this.running = new AtomicBoolean(true);
		this.flushSubTaskSnapshotProgressThreadPool = new ScheduledThreadPoolExecutor(1);
		this.flushSubTaskSnapshotProgressThreadPool.scheduleAtFixedRate(this::flushSubTaskSnapshotProgress,
				INTERVAL, INTERVAL, TimeUnit.MILLISECONDS);
	}

	public void flushSubTaskSnapshotProgress() {
		Thread.currentThread().setName("Flush-SubTask-Snapshot-Progress-" + taskDto.getName() + "(" + taskDto.getId().toHexString() + ")");
		try {
			boolean allDone = true;
			boolean hasData = false;
			TaskSnapshotProgress subTaskSnapshotProgress = TaskSnapshotProgress.getSnapshotSubTaskProgress(taskDto.getId().toHexString());
			int limit = 20;
			int pageNum = 1;

			while (true) {
				Query query = new Query(Criteria.where("subTaskId").is(taskDto.getId().toHexString())
						.and("type").is(TaskSnapshotProgress.ProgressType.EDGE_PROGRESS.name()));
				query.skip((long) (pageNum - 1) * limit);
				query.limit(limit);

				List<TaskSnapshotProgress> edgeSnapshotProgresses = clientMongoOperator.find(query, ConnectorConstant.SUBTASK_PROGRESS, TaskSnapshotProgress.class);
				if (CollectionUtils.isEmpty(edgeSnapshotProgresses) || Thread.currentThread().isInterrupted()) {
					break;
				}
				hasData = true;
				pageNum++;
				subTaskSnapshotProgress.setTotalTaleNum(subTaskSnapshotProgress.getTotalTaleNum() + edgeSnapshotProgresses.size());
				for (TaskSnapshotProgress edgeSnapshotProgress : edgeSnapshotProgresses) {
					Long waitForRunNumber = edgeSnapshotProgress.getWaitForRunNumber();
					if (waitForRunNumber > 0) {
						// Accumulate the number of rows to be synchronized
						if (subTaskSnapshotProgress.getWaitForRunNumber() < 0) {
							subTaskSnapshotProgress.setWaitForRunNumber(0L);
						}
						subTaskSnapshotProgress.setWaitForRunNumber(subTaskSnapshotProgress.getWaitForRunNumber() + waitForRunNumber);
					}
					Long finishNumber = edgeSnapshotProgress.getFinishNumber();
					if (finishNumber > 0) {
						// Accumulate the number of completed synchronization lines
						subTaskSnapshotProgress.setFinishNumber(subTaskSnapshotProgress.getFinishNumber() + finishNumber);
					}
					if (waitForRunNumber >= 0 && finishNumber >= waitForRunNumber) {
						// Accumulate the number of synchronized tables
						subTaskSnapshotProgress.setCompleteTaleNum(subTaskSnapshotProgress.getCompleteTaleNum() + 1);
					}
					if (!edgeSnapshotProgress.getStatus().equals(TaskSnapshotProgress.ProgressStatus.done)) {
						allDone = false;
					}
				}
			}

			if (allDone && hasData) {
				subTaskSnapshotProgress.setEndTs(System.currentTimeMillis());
			}
			Query query = new Query(Criteria.where("subTaskId").is(taskDto.getId().toHexString())
					.and("type").is(TaskSnapshotProgress.ProgressType.TASK_PROGRESS.name()));
			List<BatchOperationDto> batchOperationDtoList = new ArrayList<>();
			BatchOperationDto batchOperationDto = new BatchOperationDto();
			batchOperationDto.setWhere(query.getQueryObject().toJson());
			batchOperationDto.setDocument(subTaskSnapshotProgress);
			batchOperationDto.setOp(BatchOperationDto.BatchOp.upsert);
			batchOperationDtoList.add(batchOperationDto);
			clientMongoOperator.batch(batchOperationDtoList, ConnectorConstant.SUBTASK_PROGRESS, r -> !running.get());

			if (allDone && hasData) {
				flushSubTaskSnapshotProgressThreadPool.shutdownNow();
			}
		} catch (Exception e) {
			logger.warn("Flush sub task snapshot progress error: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
		}
	}

	public void incrementEdgeFinishNumber(String srcTableName) {
		if (StringUtils.isBlank(srcTableName)) {
			return;
		}
		if (MapUtils.isEmpty(snapshotEdgeProgressMap)) {
			return;
		}
		try {
			while (true) {
				try {
					if (lock.tryLock(1L, TimeUnit.SECONDS)) {
						break;
					}
				} catch (InterruptedException e) {
					break;
				}
			}
			List<TaskSnapshotProgress> subTaskSnapshotProgresses = snapshotEdgeProgressMap.getOrDefault(srcTableName, new ArrayList<>());
			if (CollectionUtils.isEmpty(subTaskSnapshotProgresses)) {
				return;
			}
			for (TaskSnapshotProgress progress : subTaskSnapshotProgresses) {
				Long waitForRunNumber = progress.getWaitForRunNumber();
				Long finishNumber = progress.getFinishNumber();
				if (finishNumber >= waitForRunNumber) {
					continue;
				}
				progress.setFinishNumber(finishNumber + 1);
				if (StringUtils.isBlank(currentTableName) || !currentTableName.equals(srcTableName)) {
					progress.setStatus(TaskSnapshotProgress.ProgressStatus.running);
				}
			}
			incrementEdgeProgressMap.put(srcTableName, subTaskSnapshotProgresses);
			this.currentTableName = srcTableName;
		} finally {
			Optional.ofNullable(lock).ifPresent(Lock::unlock);
		}
	}

	public void startAutoFlushEdgeProgresses(Node<?> srcNode) {
		flushEdgeSnapshotProgressThreadPool = new ScheduledThreadPoolExecutor(1);
		flushEdgeSnapshotProgressThreadPool.scheduleAtFixedRate(() -> {
					Thread.currentThread().setName("Flush-Snapshot-Edge-Progress-" + taskDto.getName() + "(" + taskDto.getId().toHexString() + ")"
							+ "-" + srcNode.getId());
					flushSnapshotEdgeProgress();
				},
				INTERVAL, INTERVAL, TimeUnit.MILLISECONDS);
	}

	public void flushSnapshotEdgeProgress() {
		try {
			while (true) {
				try {
					if (lock.tryLock(1L, TimeUnit.SECONDS)) {
						break;
					}
				} catch (InterruptedException e) {
					break;
				}
			}
			if (MapUtils.isEmpty(incrementEdgeProgressMap)) {
				return;
			}
			List<BatchOperationDto> batchOperationDtoList = new ArrayList<>();
			for (List<TaskSnapshotProgress> value : incrementEdgeProgressMap.values()) {
				for (TaskSnapshotProgress taskSnapshotProgress : value) {
					if (taskSnapshotProgress.getWaitForRunNumber() >= 0
							&& taskSnapshotProgress.getFinishNumber() >= taskSnapshotProgress.getWaitForRunNumber()) {
						taskSnapshotProgress.setStatus(TaskSnapshotProgress.ProgressStatus.done);
					}
					batchOperationDtoList.add(wrapBatchOperation(taskSnapshotProgress, BatchOperationDto.BatchOp.update));
				}
			}
			clientMongoOperator.batch(batchOperationDtoList, ConnectorConstant.SUBTASK_PROGRESS, r -> !running.get());
			incrementEdgeProgressMap.clear();
		} finally {
			Optional.ofNullable(lock).ifPresent(lock -> {
				if (((ReentrantLock) lock).isHeldByCurrentThread()) {
					lock.unlock();
				}
			});
		}
	}

	private void generateAndWriteEdgeSnapshotProgresses(Node<?> srcNode) {
		generateEdgeSnapshotProgresses(srcNode);
		writeEdgeSnapshotProgresses();
	}

	private void writeEdgeSnapshotProgresses() {
		writeEdgeSnapshotProgresses(this.snapshotEdgeProgressMap);
	}

	private void writeEdgeSnapshotProgresses(Map<String, List<TaskSnapshotProgress>> snapshotEdgeProgressList) {
		List<BatchOperationDto> batchOperationDtoList = new ArrayList<>();
		for (List<TaskSnapshotProgress> value : snapshotEdgeProgressList.values()) {
			for (TaskSnapshotProgress taskSnapshotProgress : value) {
				batchOperationDtoList.add(wrapBatchOperation(taskSnapshotProgress, BatchOperationDto.BatchOp.upsert));
				if (batchOperationDtoList.size() == BATCH_SIZE) {
					clientMongoOperator.batch(batchOperationDtoList, ConnectorConstant.SUBTASK_PROGRESS, o -> !running.get());
					batchOperationDtoList.clear();
				}
			}
		}
		if (CollectionUtils.isNotEmpty(batchOperationDtoList)) {
			clientMongoOperator.batch(batchOperationDtoList, ConnectorConstant.SUBTASK_PROGRESS, o -> !running.get());
			batchOperationDtoList.clear();
		}
	}

	private BatchOperationDto wrapBatchOperation(TaskSnapshotProgress taskSnapshotProgress, BatchOperationDto.BatchOp batchOp) {
		BatchOperationDto batchOperationDto;
		try {
			Query edgeQuery = getEdgeQuery(taskSnapshotProgress);
			batchOperationDto = new BatchOperationDto();
			batchOperationDto.setWhere(edgeQuery.getQueryObject().toJson());
			batchOperationDto.setDocument(taskSnapshotProgress);
			batchOperationDto.setOp(batchOp);
		} catch (Exception e) {
			throw new RuntimeException("Wrap snapshot edge progress to document error: " + e.getMessage() + "\nData: " + taskSnapshotProgress, e);
		}
		return batchOperationDto;
	}

	private void countAndUpdateEdgeSnapshotProgresses() {
		List<BatchOperationDto> batchList = new ArrayList<>();
		connectionsMap = new HashMap<>();
		for (String srcTableName : this.snapshotEdgeProgressMap.keySet()) {
			if (!running.get() || Thread.currentThread().isInterrupted()) {
				break;
			}
			List<TaskSnapshotProgress> list = this.snapshotEdgeProgressMap.get(srcTableName);
			if (CollectionUtils.isEmpty(list)) {
				continue;
			}
			TaskSnapshotProgress snapshotEdgeProgress = list.get(0);
			String srcConnId = snapshotEdgeProgress.getSrcConnId();
			Connections srcConn;
			if (connectionsMap.containsKey(srcConnId)) {
				srcConn = connectionsMap.get(srcConnId);
			} else {
				Query query = new Query(Criteria.where("_id").is(srcConnId));
				query.fields().exclude("schema");
				srcConn = clientMongoOperator.findOne(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
				srcConn.decodeDatabasePassword();
				connectionsMap.put(srcConnId, srcConn);
			}

			long count = -2;
			String errorMsg = "";
			if (null != source) {
				try {
					Object countObj = ReflectUtil.invokeInterfaceMethod(
							source, "io.tapdata.BaseExtend;io.tapdata.ConnectorExtend;io.tapdata.TargetExtend", "count",
							srcTableName, srcConn);
					if (null != countObj) {
						count = Long.parseLong(countObj.toString());
					}
				} catch (Exception e) {
					logger.warn("Count " + srcConn.getName() + "." + srcTableName + " failed: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					errorMsg = e.getMessage() + "\n" + Log4jUtil.getStackString(e);
				}
			} else if (null != connectorNode) {
				BatchCountFunction batchCountFunction = connectorNode.getConnectorFunctions().getBatchCountFunction();
				if (null != batchCountFunction) {
					AtomicLong pdkCount = new AtomicLong();
					AtomicReference<TapTable> tapTable = new AtomicReference<>();
					Optional.ofNullable(tapTableMap).ifPresent(tm -> tapTable.set(tapTableMap.get(srcTableName)));
					if (null == tapTable.get()) {
						tapTable.set(new TapTable(srcTableName));
					}
					try {
						PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_BATCH_COUNT,
								() -> pdkCount.set(batchCountFunction.count(connectorNode.getConnectorContext(), tapTable.get())), TAG);
						count = pdkCount.get();
					} catch (Exception e) {
						RuntimeException runtimeException = new RuntimeException("Count " + tapTable.get().getId() + " failed: " + e.getMessage(), e);
						logger.warn(runtimeException.getMessage() + "\n" + Log4jUtil.getStackString(e));
						throw runtimeException;
					}
				} else {
					errorMsg = "Counting is not supported for database " + srcConn.getDatabase_type();
				}
			}
			for (TaskSnapshotProgress taskSnapshotProgress : list) {
				taskSnapshotProgress.setWaitForRunNumber(count);
				if (StringUtils.isNotBlank(errorMsg)) {
					taskSnapshotProgress.setErrorMsg(errorMsg);
					taskSnapshotProgress.setStatus(TaskSnapshotProgress.ProgressStatus.done);
				} else {
					if (count == 0L) {
						taskSnapshotProgress.setStatus(TaskSnapshotProgress.ProgressStatus.done);
					}
				}
				batchList.add(wrapBatchOperation(taskSnapshotProgress, BatchOperationDto.BatchOp.update));
				if (batchList.size() == BATCH_SIZE) {
					clientMongoOperator.batch(batchList, ConnectorConstant.SUBTASK_PROGRESS, r -> !running.get());
					batchList.clear();
				}
			}
		}
		if (CollectionUtils.isNotEmpty(batchList)) {
			clientMongoOperator.batch(batchList, ConnectorConstant.SUBTASK_PROGRESS, r -> !running.get());
			batchList.clear();
		}
	}

	private Query getEdgeQuery(TaskSnapshotProgress taskSnapshotProgress) {
		return new Query(Criteria.where("taskId").is(taskSnapshotProgress.getTaskId())
				.and("srcNodeId").is(taskSnapshotProgress.getSrcNodeId())
				.and("tgtNodeId").is(taskSnapshotProgress.getTgtNodeId())
				.and("srcTableName").is(taskSnapshotProgress.getSrcTableName())
				.and("tgtTableName").is(taskSnapshotProgress.getTgtTableName())
				.and("type").is(TaskSnapshotProgress.ProgressType.EDGE_PROGRESS.name()));
	}

	private void generateEdgeSnapshotProgresses(Node<?> srcNode) {
		if (!srcNode.isDataNode()) {
			return;
		}
		List<TaskSnapshotProgress> srcEdgeProgressList = clientMongoOperator.find(new Query(Criteria.where("srcNodeId").is(srcNode.getId())), ConnectorConstant.SUBTASK_PROGRESS, TaskSnapshotProgress.class);
		List<Node<?>> successors = GraphUtil.successors(srcNode, Node::isDataNode);
		for (Node<?> tgtNode : successors) {
			if (srcNode instanceof TableNode && tgtNode instanceof TableNode) {
				List<TaskSnapshotProgress> list;
				if (snapshotEdgeProgressMap.containsKey(((TableNode) srcNode).getTableName())) {
					list = snapshotEdgeProgressMap.get(((TableNode) srcNode).getTableName());
				} else {
					list = new ArrayList<>();
					snapshotEdgeProgressMap.put(((TableNode) srcNode).getTableName(), list);
				}
				TaskSnapshotProgress foundEdgeProgress = null;
				if (CollectionUtils.isNotEmpty(srcEdgeProgressList)) {
					foundEdgeProgress = srcEdgeProgressList.stream().filter(s -> s.getTgtNodeId().equals(tgtNode.getId())).findFirst().orElse(null);
				}
				if (null != foundEdgeProgress) {
					list.add(foundEdgeProgress);
				} else {
					list.add(TaskSnapshotProgress.getSnapshotEdgeProgress(
							taskDto.getId().toHexString(),
							srcNode.getId(), tgtNode.getId(),
							((TableNode) srcNode).getConnectionId(), ((TableNode) tgtNode).getConnectionId(),
							((TableNode) srcNode).getTableName(), ((TableNode) tgtNode).getTableName()));
				}
			} else if (srcNode instanceof DatabaseNode && tgtNode instanceof DatabaseNode) {
				List<SyncObjects> syncObjects = ((DatabaseNode) tgtNode).getSyncObjects();
				if (CollectionUtils.isEmpty(syncObjects)) {
					continue;
				}
				SyncObjects objects = syncObjects.stream().filter(s -> s.getType().equals("table")).findFirst().orElse(null);
				if (null == objects) {
					continue;
				}
				LinkedHashMap<String, String> tableNameRelation = objects.getTableNameRelation();
				if (tableNameRelation == null) {
					continue;
				}

				for (Map.Entry<String, String> entry : tableNameRelation.entrySet()) {
					String srcTableName = entry.getKey();
					String tgtTableName = entry.getValue();
					List<TaskSnapshotProgress> list;
					if (snapshotEdgeProgressMap.containsKey(srcTableName)) {
						list = snapshotEdgeProgressMap.get(srcTableName);
					} else {
						list = new ArrayList<>();
						snapshotEdgeProgressMap.put(srcTableName, list);
					}
					TaskSnapshotProgress tgtEdgeProgress = null;
					if (CollectionUtils.isNotEmpty(srcEdgeProgressList)) {
						tgtEdgeProgress = srcEdgeProgressList.stream().filter(s -> s.getSrcTableName().equals(srcTableName) && s.getTgtTableName().equals(tgtTableName)).findFirst().orElse(null);
					}
					if (null != tgtEdgeProgress) {
						list.add(tgtEdgeProgress);
					} else {
						list.add(TaskSnapshotProgress.getSnapshotEdgeProgress(
								taskDto.getId().toHexString(),
								srcNode.getId(), tgtNode.getId(),
								((DatabaseNode) srcNode).getConnectionId(), ((DatabaseNode) tgtNode).getConnectionId(),
								srcTableName, tgtTableName));
					}
				}
			} else {
				logger.warn("Init snapshot progress failed, found invalid linking, "
						+ srcNode.getClass().getSimpleName() + "(" + srcNode.getName() + "_" + srcNode.getId() + ")->"
						+ tgtNode.getClass().getSimpleName() + "(" + tgtNode.getName() + "_" + tgtNode.getId() + ")");
			}
		}
	}

	@Override
	public void close() {
		Optional.ofNullable(progressThreadPool).ifPresent(ExecutorService::shutdownNow);
		if (null != flushEdgeSnapshotProgressThreadPool) {
			flushEdgeSnapshotProgressThreadPool.shutdownNow();
			flushSnapshotEdgeProgress();
		}
		if (null != flushSubTaskSnapshotProgressThreadPool) {
			flushSubTaskSnapshotProgressThreadPool.shutdownNow();
			flushSubTaskSnapshotProgress();
		}
		Optional.ofNullable(running).ifPresent(r -> r.compareAndSet(true, false));
	}
}
