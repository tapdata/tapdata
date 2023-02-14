package io.tapdata.flow.engine.V2.task.cleaner;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.AggregationProcessorNode;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.constant.Level;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import io.tapdata.aspect.TaskResetAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastCustomProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastMergeNode;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation.HazelcastMultiAggregatorProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.common.ReleaseExternalFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-10-13 16:34
 **/
public abstract class TaskCleaner {
	private static final Logger logger = LogManager.getLogger(TaskCleaner.class);
	private static final String TAG = TaskCleaner.class.getSimpleName();
	protected TaskCleanerContext taskCleanerContext;
	protected TaskCleanerReporter taskCleanerReporter;
	protected TaskDto taskDto;
	protected AtomicInteger allCounter = new AtomicInteger();
	protected AtomicInteger succeedCounter = new AtomicInteger();
	protected AtomicInteger failedCounter = new AtomicInteger();
	protected long elapsedTime = 0L;
	protected AtomicBoolean hasError = new AtomicBoolean();

	protected TaskCleaner(TaskCleanerContext taskCleanerContext) {
		this.taskCleanerContext = taskCleanerContext;
		this.taskCleanerReporter = new TaskCleanerReporterImplV1();
	}

	public void clean() throws TaskCleanerException {
		// Find task dto by task id
		taskDto = findTaskDto();
		startClean();
		if (null == taskDto.getDag()) {
			logger.warn("Task[{}]'s dag is empty, will not do any clean operation", taskDto.getName());
			return;
		}
		if (CollectionUtils.isEmpty(taskDto.getDag().getNodes())) {
			logger.warn("Task[{}]'s node list is empty, will not do any clean operation", taskDto.getName());
			return;
		}
		// Loop nodes
		List<Node> nodes = taskDto.getDag().getNodes();
		for (Node node : nodes) {
			if (null == node) {
				logger.warn("Reset/Delete task {}({}) found an empty node, will skip it", taskDto.getName(), taskDto.getId());
				continue;
			}
			// Clean node resource
			try {
				if (node instanceof TableNode || node instanceof DatabaseNode || node instanceof LogCollectorNode) {
					dataNodeDestroy(node);
				} else if (node instanceof MergeTableNode) {
					mergeNodeDestroy(node);
				} else if (node instanceof AggregationProcessorNode) {
					aggregateNodeDestroy(node);
				} else if (node instanceof CustomProcessorNode) {
					customNodeDestroy(node);
				} else if (node instanceof JoinProcessorNode) {
					joinNodeDestroy(node);
				}
			} catch (Throwable throwable) {
				errorHandle(node, throwable);
			}
		}
		endClean();
	}

	private void errorHandle(Node node, Throwable throwable) {
		unknownError(node, throwable);
	}

	private void joinNodeDestroy(Node node) {
		long startTs = System.currentTimeMillis();
		try {
			HazelcastJoinProcessor.clearCache(node);
			succeed(node, NodeResetDesc.task_reset_join_node, (System.currentTimeMillis() - startTs));
		} catch (Throwable e) {
			String msg = String.format("Clean join node cache data occur an error: %s\n Task: %s(%s), node: %s(%s)", e.getMessage(), taskDto.getName(), taskDto.getId(), node.getName(), node.getId());
			TaskCleanerException taskCleanerException = new TaskCleanerException(msg, e, true);
			failed(node, NodeResetDesc.task_reset_join_node, (System.currentTimeMillis() - startTs), taskCleanerException);
		}
	}

	private void customNodeDestroy(Node node) {
		long startTs = System.currentTimeMillis();
		try {
			HazelcastCustomProcessor.clearStateMap(node.getId());
			succeed(node, NodeResetDesc.task_reset_custom_node, (System.currentTimeMillis() - startTs));
		} catch (Exception e) {
			String msg = String.format("Clean custom node state data occur an error: %s, Task: %s(%s), node: %s(%s)", e.getMessage(), taskDto.getName(), taskDto.getId(), node.getName(), node.getId());
			TaskCleanerException taskCleanerException = new TaskCleanerException(msg, e, true);
			failed(node, NodeResetDesc.task_reset_custom_node, (System.currentTimeMillis() - startTs), taskCleanerException);
		}
	}

	private void aggregateNodeDestroy(Node<?> node) {
		long startTs = System.currentTimeMillis();
		try {
			HazelcastMultiAggregatorProcessor.clearCache(node.getId(), HazelcastUtil.getInstance());
			succeed(node, NodeResetDesc.task_reset_aggregate_node, (System.currentTimeMillis() - startTs));
		} catch (Throwable e) {
			String msg = String.format("Clean aggregate node cache data occur an error: %s\n Task: %s(%s), node: %s(%s)", e.getMessage(), taskDto.getName(), taskDto.getId(), node.getName(), node.getId());
			TaskCleanerException taskCleanerException = new TaskCleanerException(msg, e, true);
			failed(node, NodeResetDesc.task_reset_aggregate_node, (System.currentTimeMillis() - startTs), taskCleanerException);
		}
	}

	private void mergeNodeDestroy(Node<?> node) {
		long startTs = System.currentTimeMillis();
		try {
			HazelcastMergeNode.clearCache(node);
			succeed(node, NodeResetDesc.task_reset_merge_node, (System.currentTimeMillis() - startTs));
		} catch (Throwable e) {
			String msg = String.format("Clean merge node cache data occur an error: %s\n Task: %s(%s), node: %s(%s)", e.getMessage(), taskDto.getName(), taskDto.getId(), node.getName(), node.getId());
			TaskCleanerException taskCleanerException = new TaskCleanerException(msg, e, true);
			failed(node, NodeResetDesc.task_reset_merge_node, (System.currentTimeMillis() - startTs), taskCleanerException);
		}
	}

	private void dataNodeDestroy(Node<?> node) {
		long startTs = System.currentTimeMillis();
		String connectionId = null;
		Connections connections = null;
		DatabaseTypeEnum.DatabaseType databaseType;
		ClientMongoOperator clientMongoOperator = taskCleanerContext.getClientMongoOperator();
		if (node instanceof TableNode) {
			connectionId = ((TableNode) node).getConnectionId();
		} else if (node instanceof DatabaseNode) {
			connectionId = ((DatabaseNode) node).getConnectionId();
		} else if (node instanceof LogCollectorNode) {
			List<String> connectionIds = ((LogCollectorNode) node).getConnectionIds();
			if (CollectionUtils.isNotEmpty(connectionIds)) {
				connectionId = connectionIds.get(0);
			} else {
				throw new RuntimeException("Node " + node.getName() + "(" + node.getId() + ") not contain connection id");
			}
		}
		if (StringUtils.isNotBlank(connectionId)) {
			connections = getConnection(connectionId);
		}
		if (null == connections) return;
		databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
		if (null == databaseType) return;
		Connections finalConnections = connections;
		PdkStateMap pdkStateMap = new PdkStateMap(node.getId(), HazelcastTaskService.getHazelcastInstance(), PdkStateMap.StateMapMode.HTTP_TM);
		PdkStateMap globalStateMap = PdkStateMap.globalStateMap(HazelcastTaskService.getHazelcastInstance());
		PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator,
				databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
		TapTableMap<String, TapTable> tapTableMapByNodeId = TapTableUtil.getTapTableMapByNodeId(node.getId());
		PdkTableMap pdkTableMap = new PdkTableMap(tapTableMapByNodeId);
		String associateId = this.getClass().getSimpleName() + "-" + node.getId();
		try {
			AtomicReference<ConnectorNode> connectorNode = new AtomicReference<>();
			try {
				connectorNode.set(buildConnectorNode(databaseType, finalConnections, pdkStateMap, globalStateMap, pdkTableMap, associateId));
			} catch (Throwable throwable) {
				CommonUtils.ignoreAnyError(() -> PDKIntegration.releaseAssociateId(associateId), TAG);
				connectorNode.set(buildConnectorNode(databaseType, finalConnections, pdkStateMap, globalStateMap, pdkTableMap, associateId));
			}

			try {
				ReleaseExternalFunction releaseExternalFunction = connectorNode.get().getConnectorFunctions().getReleaseExternalFunction();
				if (releaseExternalFunction != null) {
					try {
						PDKInvocationMonitor.invoke(connectorNode.get(), PDKMethod.RELEASE_EXTERNAL, () -> releaseExternalFunction.release(connectorNode.get().getConnectorContext()), TAG);
						succeed(node, NodeResetDesc.task_reset_pdk_node_external_resource, (System.currentTimeMillis() - startTs));
					} catch (Throwable e) {
						TapNodeSpecification specification = connectorNode.get().getConnectorContext().getSpecification();
						String msg = String.format("Call pdk function releaseExternalFunction occur an error: %s\n Task: %s(%s), node: %s(%s), pdk connector: %s-%s-%s",
								e.getMessage(), taskDto.getName(), taskDto.getId(), node.getName(), node.getId(), specification.getGroup(), specification.getId(), specification.getVersion());
						TaskCleanerException taskCleanerException = new TaskCleanerException(msg, e, true);
						failed(node, NodeResetDesc.task_reset_pdk_node_external_resource, (System.currentTimeMillis() - startTs), taskCleanerException);
					}
				}
			} catch (Throwable e) {
				TapNodeSpecification specification = connectorNode.get().getConnectorContext().getSpecification();
				String msg = String.format("Call pdk function init occur an error: %s\n Task: %s(%s), node: %s(%s), pdk connector: %s-%s-%s",
						e.getMessage(), taskDto.getName(), taskDto.getId(), node.getName(), node.getId(), specification.getGroup(), specification.getId(), specification.getVersion());
				TaskCleanerException taskCleanerException = new TaskCleanerException(msg, e, true);
				failed(node, NodeResetDesc.task_reset_pdk_node_external_resource, (System.currentTimeMillis() - startTs), taskCleanerException);
			}

			AspectUtils.executeAspect(TaskResetAspect.class, () -> new TaskResetAspect().task(taskDto));
			TapConnectorContext connectorContext = connectorNode.get().getConnectorContext();
			if (connectorContext != null) {
				KVMap<Object> stateMap = connectorContext.getStateMap();
				if (stateMap != null) {
					startTs = System.currentTimeMillis();
					try {
						stateMap.reset();
						succeed(node, NodeResetDesc.task_reset_pdk_node_state, (System.currentTimeMillis() - startTs));
					} catch (Throwable e) {
						TapNodeSpecification specification = connectorNode.get().getConnectorContext().getSpecification();
						String msg = String.format("Clean pdk state data occur an error: %s\n Task: %s(%s), node: %s(%s), pdk connector: %s-%s-%s",
								e.getMessage(), taskDto.getName(), taskDto.getId(), node.getName(), node.getId(), specification.getGroup(), specification.getId(), specification.getVersion());
						TaskCleanerException taskCleanerException = new TaskCleanerException(msg, e, true);
						failed(node, NodeResetDesc.task_reset_pdk_node_state, (System.currentTimeMillis() - startTs), taskCleanerException);
					}
				}
			}
		} finally {
			CommonUtils.ignoreAnyError(tapTableMapByNodeId::reset, TAG);
			CommonUtils.ignoreAnyError(() -> PDKIntegration.releaseAssociateId(associateId), TAG);
		}
	}

	private ConnectorNode buildConnectorNode(DatabaseTypeEnum.DatabaseType databaseType, Connections finalConnections, PdkStateMap pdkStateMap, PdkStateMap globalStateMap, PdkTableMap pdkTableMap, String associateId) {
		return PDKIntegration.createConnectorBuilder()
				.withDagId(taskDto.getId().toHexString())
				.withAssociateId(associateId)
				.withConnectionConfig(new DataMap() {{
					putAll(finalConnections.getConfig());
				}})
				.withGroup(databaseType.getGroup())
				.withVersion(databaseType.getVersion())
				.withPdkId(databaseType.getPdkId())
				.withTableMap(pdkTableMap)
				.withStateMap(pdkStateMap)
				.withGlobalStateMap(globalStateMap)
				.build();
	}

	private Connections getConnection(String connectionId) {
		ClientMongoOperator clientMongoOperator = taskCleanerContext.getClientMongoOperator();
		Connections connections = clientMongoOperator.findOne(
				new Query(where("_id").is(connectionId)),
				ConnectorConstant.CONNECTION_COLLECTION,
				Connections.class
		);
		connections.decodeDatabasePassword();
		connections.initCustomTimeZone();
		return connections;
	}

	private TaskDto findTaskDto() throws TaskCleanerException {
		TaskDto taskDto;
		String taskId = taskCleanerContext.getTaskId();
		ClientMongoOperator clientMongoOperator = taskCleanerContext.getClientMongoOperator();
		try {
			Query query = Query.query(Criteria.where("_id").is(taskId));
			query.fields().include("dag").include("_id").include("name");
			taskDto = clientMongoOperator.findOne(Query.query(Criteria.where("_id").is(taskId)), ConnectorConstant.TASK_COLLECTION, TaskDto.class);
		} catch (Exception e) {
			throw new TaskCleanerException(String.format("Find task by id [%s] failed: %s", taskId, e.getMessage()), e);
		}
		if (null == taskDto) {
			throw new TaskCleanerException(String.format("Find task by id [%s] result cannot be null", taskId));
		}

		return taskDto;
	}

	protected TaskResetEventDto succeed(Node node, NodeResetDesc nodeResetDesc, Long elapsedTime) {
		TaskResetEventDto taskResetEventDto = genTaskResetEvent(node, nodeResetDesc, elapsedTime);
		return addEvent(taskResetEventDto);
	}

	protected TaskResetEventDto failed(Node node, NodeResetDesc nodeResetDesc, Long elapsedTime, Throwable throwable) {
		hasError.set(true);
		TaskResetEventDto taskResetEventDto = genTaskResetEvent(node, nodeResetDesc, elapsedTime, throwable);
		return addEvent(taskResetEventDto);
	}

	private TaskResetEventDto addEvent(TaskResetEventDto taskResetEventDto) {
		taskCleanerReporter.addEvent(taskCleanerContext.getClientMongoOperator(), taskResetEventDto);
		allCounter.incrementAndGet();
		if (taskResetEventDto.getStatus().equals(TaskResetEventDto.ResetStatusEnum.SUCCEED)) {
			succeedCounter.incrementAndGet();
		} else if (taskResetEventDto.getStatus().equals(TaskResetEventDto.ResetStatusEnum.FAILED)) {
			failedCounter.incrementAndGet();
		}
		if (null != taskResetEventDto.getElapsedTime()) {
			this.elapsedTime += taskResetEventDto.getElapsedTime();
		}
		return taskResetEventDto;
	}

	private TaskResetEventDto genTaskResetEvent(Node node, NodeResetDesc nodeResetDesc, Long elapsedTime) {
		return genTaskResetEvent(node, nodeResetDesc, elapsedTime, null);
	}

	private TaskResetEventDto genTaskResetEvent(Node node, NodeResetDesc nodeResetDesc, Long elapsedTime, Throwable throwable) {
		TaskResetEventDto taskResetEventDto = new TaskResetEventDto();
		taskResetEventDto.setTaskId(taskDto.getId().toHexString());
		taskResetEventDto.setDescribe(nodeResetDesc.name());
		taskResetEventDto.setNodeId(node.getId());
		taskResetEventDto.setNodeName(node.getName());
		taskResetEventDto.setElapsedTime(elapsedTime);
		if (null != throwable) {
			taskResetEventDto.failed(throwable);
		} else {
			taskResetEventDto.succeed();
		}
		return taskResetEventDto;
	}

	private void startClean() {
		TaskResetEventDto taskResetEventDto = new TaskResetEventDto();
		taskResetEventDto.setTaskId(taskDto.getId().toHexString());
		taskResetEventDto.setLevel(Level.INFO);
		taskResetEventDto.setDescribe(NodeResetDesc.task_reset_start.name());
		taskResetEventDto.setStatus(TaskResetEventDto.ResetStatusEnum.START);
		taskCleanerReporter.addEvent(taskCleanerContext.getClientMongoOperator(), taskResetEventDto);
	}

	private void endClean() {
		TaskResetEventDto taskResetEventDto = new TaskResetEventDto();
		taskResetEventDto.setTaskId(taskDto.getId().toHexString());
		taskResetEventDto.setDescribe(NodeResetDesc.task_reset_end.name());
		taskResetEventDto.setTotalEvent(allCounter.get());
		taskResetEventDto.setSucceedEvent(succeedCounter.get());
		taskResetEventDto.setFailedEvent(failedCounter.get());
		taskResetEventDto.setElapsedTime(elapsedTime);
		taskResetEventDto.setLevel(Level.INFO);
		if (hasError.get()) {
			taskResetEventDto.setStatus(TaskResetEventDto.ResetStatusEnum.TASK_FAILED);
		} else {
			taskResetEventDto.setStatus(TaskResetEventDto.ResetStatusEnum.TASK_SUCCEED);
		}
		taskCleanerReporter.addEvent(taskCleanerContext.getClientMongoOperator(), taskResetEventDto);
	}

	private void unknownError(Node node, Throwable throwable) {
		TaskResetEventDto taskResetEventDto = new TaskResetEventDto();
		taskResetEventDto.setTaskId(taskDto.getId().toHexString());
		taskResetEventDto.setNodeId(node.getId());
		taskResetEventDto.setNodeName(node.getName());
		taskResetEventDto.setDescribe(NodeResetDesc.unknown_error.name());
		taskResetEventDto.failed(throwable);
		taskCleanerReporter.addEvent(taskCleanerContext.getClientMongoOperator(), taskResetEventDto);
	}
}
