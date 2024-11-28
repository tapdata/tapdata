package io.tapdata.flow.engine.V2.task.preview;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.PreviewTargetNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.utils.StopWatch;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.task.preview.entity.PreviewConnectionInfo;
import io.tapdata.flow.engine.V2.task.preview.tasklet.PreviewMergeReadTasklet;
import io.tapdata.flow.engine.V2.task.preview.tasklet.PreviewNormalReadTasklet;
import io.tapdata.flow.engine.V2.task.preview.tasklet.PreviewReadTasklet;
import io.tapdata.log.EmptyLog;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.schema.TapTableMap;
import io.tapdata.service.skeleton.annotation.RemoteService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2024-09-18 17:08
 **/
@RemoteService
public class TaskPreviewService {
	public static final String TAG = TaskPreviewService.class.getSimpleName();
	private static final Logger logger = LogManager.getLogger(TaskPreviewService.class);
	private static final Map<String, TaskPreviewInstance> taskPreviewInstanceMap = new ConcurrentHashMap<>();
	public static final int DEFAULT_PREVIEW_ROWS = 1;

	public TaskPreviewResultVO preview(String taskJson, List<String> includeNodeIds, Integer previewRows) {
		StopWatch stopWatch = StopWatch.create(String.join("_", TAG, System.currentTimeMillis() + ""));
		TaskDto taskDto;
		try {
			stopWatch.start("parseTaskJson");
			taskDto = parseTaskJson(taskJson);
		} catch (TaskPreviewParseException e) {
			logger.error("Parse task json failed: {}", taskJson, e);
			return TaskPreviewResultVO.parseFailed(e);
		}
		stopWatch.start("before");
		try {
			validateTask(taskDto);
		} catch (TaskPreviewInvalidException e) {
			logger.error(e);
			return TaskPreviewResultVO.invalid(taskDto, e.getMessage());
		}
		if (canSkipPreview(taskDto)) {
			return new TaskPreviewResultVO(taskDto);
		}
		handleTaskBeforePreview(taskDto, includeNodeIds, previewRows);
		TaskPreviewInstance taskPreviewInstance = wrapTaskPreviewInstance(taskDto);
		String taskPreviewInstanceId = taskPreviewInstanceId(taskDto);
		taskPreviewInstanceMap.put(taskPreviewInstanceId, taskPreviewInstance);
		initGlobalVariable(taskDto);
		try {
			initNodeConnectionInfoMap(taskDto);
		} catch (Exception e) {
			logger.error("Init connector node failed, task: {}({})", taskDto.getName(), taskDto.getId().toString(), e);
			taskPreviewInstance.getTaskPreviewResultVO().failed(e);
			return taskPreviewInstance.getTaskPreviewResultVO();
		}
		stopWatch.stop();
		previewPrivate(taskDto, stopWatch);
		stopWatch.start("after");
		TaskPreviewResultVO taskPreviewResultVO = taskPreviewInstanceMap.remove(taskPreviewInstanceId).getTaskPreviewResultVO();
		clearAfterPreview(taskDto);
		stopWatch.stop();
		wrapStats(taskPreviewResultVO, stopWatch);
		return taskPreviewResultVO;
	}

	private void wrapStats(TaskPreviewResultVO taskPreviewResultVO, StopWatch stopWatch) {
		TaskPReviewStatsVO stats = taskPreviewResultVO.getStats();
		StopWatch.TaskInfo[] taskInfos = stopWatch.getTaskInfo();
		for (StopWatch.TaskInfo taskInfo : taskInfos) {
			String taskName = taskInfo.getTaskName();
			switch (taskName) {
				case "parseTaskJson":
					stats.setParseTaskJsonTaken(taskInfo.getTimeMillis());
					break;
				case "before":
					stats.setBeforeTaken(taskInfo.getTimeMillis());
					break;
				case "execTask":
					stats.setExecTaskTaken(taskInfo.getTimeMillis());
					break;
				case "stopTask":
					stats.setStopTaskTaken(taskInfo.getTimeMillis());
					break;
				case "after":
					stats.setAfterTaken(taskInfo.getTimeMillis());
					break;
				default:
					break;
			}
		}
		stats.setAllTaken(stopWatch.getTotalTimeMillis());
	}

	protected void clearAfterPreview(TaskDto taskDto) {
		TaskGlobalVariable.INSTANCE.removeTask(taskPreviewInstanceId(taskDto));
		ObsLoggerFactory.getInstance().removeFromFactory(taskDto.getTestTaskId());
	}

	private TaskPreviewInstance wrapTaskPreviewInstance(TaskDto taskDto) {
		TaskPreviewInstance taskPreviewInstance = new TaskPreviewInstance();
		taskPreviewInstance.setTaskDto(taskDto);
		TaskPreviewResultVO taskPreviewResultVO = new TaskPreviewResultVO(taskDto);
		taskPreviewResultVO.setStats(new TaskPReviewStatsVO());
		taskPreviewInstance.setTaskPreviewResultVO(taskPreviewResultVO);
		PreviewReadOperationQueue previewReadOperationQueue = new PreviewReadOperationQueue(taskDto.getPreviewRows());
		taskPreviewInstance.setPreviewReadOperationQueue(previewReadOperationQueue);
		return taskPreviewInstance;
	}

	protected void previewPrivate(TaskDto taskDto, StopWatch stopWatch) {
		stopWatch.start("execTask");
		String taskId = taskDto.getId().toHexString();
		Integer previewRows = taskDto.getPreviewRows();
		TaskPreviewInstance taskPreviewInstance = taskPreviewInstanceMap.get(taskPreviewInstanceId(taskDto));
		TaskPreviewResultVO taskPreviewResultVO = taskPreviewInstance.getTaskPreviewResultVO();
		ExecutorService previewReadTaskletExecutor = null;
		try {
			PreviewReadTasklet previewReadTasklet;
			if (null != findNode(taskDto, MergeTableNode.class)) {
				previewReadTasklet = new PreviewMergeReadTasklet();
			} else {
				previewReadTasklet = new PreviewNormalReadTasklet();
			}
			previewReadTaskletExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), r -> {
				Thread thread = new Thread(r);
				thread.setName(String.join("-", previewReadTasklet.getClass().getSimpleName(), taskId, previewRows.toString()));
				return thread;
			});
			previewReadTaskletExecutor.execute(() -> {
				long startMs = System.currentTimeMillis();
				try {
					logger.info("Start preview read tasklet: {}", previewReadTasklet.getClass().getName());
					previewReadTasklet.execute(taskDto, taskPreviewInstance.getPreviewReadOperationQueue());
				} catch (TaskPreviewException e) {
					logger.error(e);
					taskPreviewResultVO.failed(e);
				}
				taskPreviewResultVO.getStats().setTaskletTaken(System.currentTimeMillis() - startMs);
			});
			HazelcastTaskService hazelcastTaskService = BeanUtil.getBean(HazelcastTaskService.class);
			if (null == hazelcastTaskService) {
				throw new TaskPreviewException("Failed to get hazelcast task service. It may be because the engine has not started completely, please wait for a while and try again.");
			}
			TaskClient<TaskDto> taskDtoTaskClient = hazelcastTaskService.startPreviewTask(taskDto);
			if (null != taskDtoTaskClient) {
				taskDtoTaskClient.join();
				stopWatch.start("stopTask");
				while (!taskDtoTaskClient.stop()) {
					TimeUnit.MILLISECONDS.sleep(1L);
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			logger.error("Execute preview task failed", e);
			taskPreviewResultVO.failed(e);
		} finally {
			Optional.ofNullable(previewReadTaskletExecutor).ifPresent(ExecutorService::shutdownNow);
		}
	}

	protected void initGlobalVariable(TaskDto taskDto) {
		Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE.getTaskGlobalVariable(taskPreviewInstanceId(taskDto));
		taskGlobalVariable.put(TaskGlobalVariable.PREVIEW_COMPLETE_KEY, false);
	}

	protected TaskDto parseTaskJson(String taskJson) throws TaskPreviewParseException {
		TaskDto taskDto;
		try {
			taskDto = JSONUtil.json2POJO(taskJson, TaskDto.class);
		} catch (IOException e) {
			throw new TaskPreviewParseException(String.format("Failed to parse task json: %s", taskJson), e);
		}
		return taskDto;
	}

	protected boolean canSkipPreview(TaskDto taskDto) {
		if (null == taskDto.getDag()) {
			return true;
		}
		DAG dag = taskDto.getDag();
		List<Node> nodes = dag.getNodes();
		return CollectionUtils.isEmpty(nodes);
	}

	protected void validateTask(TaskDto taskDto) throws TaskPreviewInvalidException {
		if (null == taskDto.getId()) {
			throw new TaskPreviewInvalidException("Task id is required");
		}
		if (isMigrateTask(taskDto)) {
			throw new TaskPreviewInvalidException("Migrate task is not supported");
		}
	}

	protected void handleTaskBeforePreview(TaskDto taskDto, List<String> includeNodeIds, Integer previewRows) {
		previewRows = (null == previewRows || previewRows < 0) ? DEFAULT_PREVIEW_ROWS : previewRows;
		taskDto.setName(String.join("_", "preview", taskDto.getName(), taskPreviewInstanceId(taskDto)));
		taskDto.setPreviewRows(previewRows);
		taskDto.setSyncType(TaskDto.SYNC_TYPE_PREVIEW);
		taskDto.setType(TaskDto.TYPE_INITIAL_SYNC);
		taskDto.setDag(handleDAG(taskDto, includeNodeIds));
		taskDto.setTestTaskId(new ObjectId().toHexString());
		taskDto.setRetryIntervalSecond(0L);
		taskDto.setMaxRetryTimeMinute(0L);
		handleMergeNode(taskDto);
	}

	protected DAG handleDAG(TaskDto taskDto, List<String> includeNodeIds) {
		Dag dag = pickIncludeNodes(taskDto.getDag(), includeNodeIds);
		dag = handlePreviewTargetNode(DAG.build(dag));
		return DAG.build(dag);
	}

	protected Dag handlePreviewTargetNode(DAG dag) {
		List<Node> allTypeTargetNodes = dag.getAllTypeTargetNodes();
		for (Node targetNode : allTypeTargetNodes) {
			PreviewTargetNode previewTargetNode = new PreviewTargetNode();
			previewTargetNode.setName(PreviewTargetNode.class.getSimpleName());
			previewTargetNode.setId(UUID.randomUUID().toString());
			List predecessors = targetNode.predecessors();
			if (CollectionUtils.isEmpty(predecessors)) {
				dag.addTargetNode(targetNode, previewTargetNode);
			} else if (targetNode instanceof TableNode || targetNode instanceof DatabaseNode) {
				dag.replaceNode(targetNode, previewTargetNode);
			} else {
				dag.addTargetNode(targetNode, previewTargetNode);
			}
		}
		return new Dag(dag.getEdges(), dag.getNodes());
	}

	private void handleMergeNode(TaskDto taskDto) {
		Node mergeTableNode = findNode(taskDto, MergeTableNode.class);
		if (null != mergeTableNode) {
			((MergeTableNode) mergeTableNode).setMergeMode(MergeTableNode.MAIN_TABLE_FIRST_MERGE_MODE);
			((MergeTableNode) mergeTableNode).setConcurrentNum(1);
		}
	}

	protected Dag pickIncludeNodes(DAG dag, List<String> includeNodeIds) {
		if (CollectionUtils.isEmpty(includeNodeIds)) {
			return dag.toDag();
		}
		List<Node> nodes = dag.getNodes();
		LinkedList<Edge> edges = dag.getEdges();
		List<Node> includeNodes = new LinkedList<>();
		LinkedList<Edge> includeEdges = new LinkedList<>();
		for (Node node : nodes) {
			if (includeNodeIds.contains(node.getId())) {
				includeNodes.add(node);
				edges.stream().filter(edge -> edge.getSource().equals(node.getId()) || edge.getTarget().equals(node.getId())).forEach(edge->{
					if (includeEdges.stream().noneMatch(e -> e.getSource().equals(edge.getSource()) && e.getTarget().equals(edge.getTarget()))
							&& includeNodeIds.contains(edge.getSource())
							&& includeNodeIds.contains(edge.getTarget())) {
						includeEdges.add(edge);
					}
				});
			}
		}
		return new Dag(includeEdges, includeNodes);
	}

	protected boolean hasNode(TaskDto taskDto, Class<? extends Node> nodeClz) {
		return taskDto.getDag().getNodes().stream().anyMatch(nodeClz::isInstance);
	}

	protected Node findNode(TaskDto taskDto, Class<? extends Node> nodeClz) {
		return taskDto.getDag().getNodes().stream().filter(nodeClz::isInstance).findFirst().orElse(null);
	}

	protected boolean isMigrateTask(TaskDto taskDto) {
		boolean hasDatabaseNode = hasNode(taskDto, DatabaseNode.class);
		String syncType = taskDto.getSyncType();
		return TaskDto.SYNC_TYPE_MIGRATE.equals(syncType) || hasDatabaseNode;
	}

	public static String taskPreviewInstanceId(TaskDto taskDto) {
		return String.join("_", taskDto.getId().toHexString(), taskDto.getTestTaskId());
	}

	public static TaskPreviewInstance taskPreviewInstance(TaskDto taskDto) {
		return taskPreviewInstanceMap.get(taskPreviewInstanceId(taskDto));
	}

	protected Map<String, TapTableMap<String, TapTable>> transformSchemaWhenPreview(TaskDto taskDto) {
		Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap;
		tapTableMapHashMap = new HashMap<>();
		boolean needTransformSchema = false;
		List<Node> sourceNodes = taskDto.getDag().getSourceNodes();
		for (Node sourceNode : sourceNodes) {
			if (sourceNode instanceof TableNode) {
				TableNode tableNode = (TableNode) sourceNode;
				String previewQualifiedName = tableNode.getPreviewQualifiedName();
				TapTable previewTapTable = tableNode.getPreviewTapTable();
				if (StringUtils.isBlank(previewQualifiedName) || null == previewTapTable) {
					needTransformSchema = true;
					break;
				}
				TapTableMap<String, TapTable> tapTableMap = TapTableMap.create(tableNode.getId());
				tapTableMap.putNew(tableNode.getTableName(), previewTapTable, previewQualifiedName);
				tapTableMapHashMap.put(tableNode.getId(), tapTableMap);
			} else {
				needTransformSchema = true;
				break;
			}
		}
		if (needTransformSchema) {
			return new HashMap<>();
		}
		return tapTableMapHashMap;
	}

	protected void initNodeConnectionInfoMap(TaskDto taskDto) {
		if (null == taskDto) {
			return;
		}
		DAG dag = taskDto.getDag();
		List<Node> sourceNodes = dag.getSourceNodes();
		TaskPreviewInstance taskPreviewInstance = taskPreviewInstanceMap.get(taskPreviewInstanceId(taskDto));
		Map<String, PreviewConnectionInfo> nodeConnectionInfoMap = taskPreviewInstance.getNodeConnectionInfoMap();
		for (Node sourceNode : sourceNodes) {
			if (!(sourceNode instanceof DataParentNode)) {
				continue;
			}
			taskPreviewInstance.getTaskPreviewResultVO().getStats().getReadStats().put(sourceNode.getId(), new TaskPreviewReadStatsVO());
			String connectionId = ((DataParentNode<?>) sourceNode).getConnectionId();
			if (nodeConnectionInfoMap.containsKey(connectionId)) {
				continue;
			}
			PreviewConnectionInfo previewConnectionInfo = new PreviewConnectionInfo();
			Map<String, TapTableMap<String, TapTable>> tableMapMap = transformSchemaWhenPreview(taskDto);
			taskPreviewInstance.setTapTableMapHashMap(tableMapMap);

			ConnectorNodeService.getInstance().globalConnectorNode(connectionId, tableMapMap.get(sourceNode.getId()), new EmptyLog(), (result, err) -> {
				if (null != err) {
					throw err;
				}
				previewConnectionInfo.setConnections(result.getConnections());
				previewConnectionInfo.setAssociateId(result.getAssociateId());
				taskPreviewInstance.getTaskPreviewResultVO().getStats().getReadStats().get(sourceNode.getId()).setInitTaken(result.getConnectorNodeInitTaken());
			});
			nodeConnectionInfoMap.put(connectionId, previewConnectionInfo);
		}
	}
}
