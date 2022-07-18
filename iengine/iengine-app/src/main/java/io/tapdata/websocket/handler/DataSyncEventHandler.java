package io.tapdata.websocket.handler;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastMergeNode;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.common.ReleaseExternalFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Map;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-05-23 12:05
 **/
@EventHandlerAnnotation(type = "dataSync")
public class DataSyncEventHandler extends BaseEventHandler {
	private static final String TAG = DataSyncEventHandler.class.getSimpleName();

	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		super.initialize(clientMongoOperator);
	}

	@Override
	public Object handle(Map event) {
		WebSocketEventResult webSocketEventResult = WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.DATA_SYNC_RESULT, event);
		try {
			String opType = (String) event.getOrDefault("opType", "");
			if (StringUtils.isBlank(opType)) {
				return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DATA_SYNC_RESULT, "Op type is empty");
			}
			switch (opType) {
				case "reset":
				case "delete":
					String taskId = (String) event.getOrDefault("taskId", "");
					if (StringUtils.isBlank(taskId)) {
						webSocketEventResult = WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DATA_SYNC_RESULT, "Task id is empty");
						break;
					}
					try {
						destroy(taskId);
					} catch (Exception e) {
						webSocketEventResult = WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DATA_SYNC_RESULT, e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
					}
					break;
				default:
					break;
			}
		} catch (Throwable e) {
			webSocketEventResult = WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DATA_SYNC_RESULT, e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
		}
		return webSocketEventResult;

	}

	private void destroy(String taskId) {
		SubTaskDto subTaskDto = clientMongoOperator.findOne(Query.query(Criteria.where("_id").is(taskId)), ConnectorConstant.SUB_TASK_COLLECTION, SubTaskDto.class);
		if (null == subTaskDto) return;
		List<Node> nodes = subTaskDto.getDag().getNodes();
		if (CollectionUtils.isEmpty(nodes)) return;
		for (Node<?> node : nodes) {
			if (node instanceof TableNode || node instanceof DatabaseNode || node instanceof LogCollectorNode) {
				dataNodeDestroy(subTaskDto, node);
			} else if (node instanceof MergeTableNode) {
				mergeNodeDestroy(node);
			} else if (node instanceof HazelCastImdgNode) {
				// TODO clear log
			}
		}
	}

	private void mergeNodeDestroy(Node<?> node) {
		HazelcastMergeNode.clearCache(node);
	}

	private void dataNodeDestroy(SubTaskDto subTaskDto, Node<?> node) {
		String connectionId = null;
		Connections connections = null;
		DatabaseTypeEnum.DatabaseType databaseType;
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
		PdkStateMap pdkStateMap = new PdkStateMap(node.getId(), HazelcastTaskService.getHazelcastInstance());
		PdkStateMap globalStateMap = PdkStateMap.globalStateMap(HazelcastTaskService.getHazelcastInstance());
		PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator,
				databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
		ConnectorNode connectorNode = PDKIntegration.createConnectorBuilder()
				.withDagId(subTaskDto.getId().toHexString())
				.withAssociateId(this.getClass().getSimpleName() + "-" + node.getId())
				.withConnectionConfig(new DataMap() {{
					putAll(finalConnections.getConfig());
				}})
				.withGroup(databaseType.getGroup())
				.withVersion(databaseType.getVersion())
				.withPdkId(databaseType.getPdkId())
				.withTableMap(new PdkTableMap(TapTableUtil.getTapTableMapByNodeId(node.getId())))
				.withStateMap(pdkStateMap)
				.withGlobalStateMap(globalStateMap)
				.build();
		try {
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);

			ReleaseExternalFunction releaseExternalFunction = connectorNode.getConnectorFunctions().getReleaseExternalFunction();
			if (releaseExternalFunction != null) {
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.RELEASE_EXTERNAL, () -> releaseExternalFunction.release(connectorNode.getConnectorContext()), TAG);
			}
			TapConnectorContext connectorContext = connectorNode.getConnectorContext();
			if (connectorContext != null) {
				KVMap<Object> stateMap = connectorContext.getStateMap();
				if (stateMap != null) {
					try {
						stateMap.reset();
					} catch (Throwable ignored) {
						TapLogger.warn(TAG, "destroy, reset stateMap failed, {}, connector {}", ignored.getMessage(), connectorContext.toString());
					}
				}
			}

			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
		} finally {
			PDKIntegration.releaseAssociateId(this.getClass().getSimpleName() + "-" + node.getId());
		}
	}

	private Connections getConnection(String connectionId) {
		final Connections connections = clientMongoOperator.findOne(
				new Query(where("_id").is(connectionId)),
				ConnectorConstant.CONNECTION_COLLECTION,
				Connections.class
		);
		connections.decodeDatabasePassword();
		connections.initCustomTimeZone();
		return connections;
	}
}
