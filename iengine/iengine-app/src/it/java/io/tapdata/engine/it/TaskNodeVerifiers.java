package io.tapdata.engine.it;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.verifier.ConnectorVerifier;
import io.tapdata.it.verifier.VerifierFactory;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.core.api.ConnectorNode;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 从运行中任务的节点反射获取 Connector 并装配旁路验证器（{@link ConnectorVerifier}）。
 * <p>
 * 反射链路：{@link ConnectorNodeService} 全局注册表（引擎
 * {@code HazelcastPdkBaseNode.createPdkConnectorNode} 创建节点时写入）
 * → 私有成员 {@code connectorNodeMap}（无公开读取 API，反射获取）
 * → 按 {@code ConnectorNode.getDagId() == taskId} 且 associateId 含节点 id
 * （associateId 格式 = 节点类名_nodeId_时间戳，见 {@code generateNodePdkAssociateId}）匹配
 * → {@code ConnectorNode.getConnector()}（TapConnector 实例）
 * → {@link VerifierFactory} 按 connector 成员类型自动装配（JdbcContext → JdbcVerifier，
 * MongoClient → MongoVerifier）。
 * <p>
 * 可用时机：ConnectorNode 在创建时即注册，但连接器成员（JdbcContext 连接池 / MongoClient）
 * 要等 {@code connectorInit} 执行完成才就绪，因此验证器需轮询等待（{@link #awaitVerifier}）。
 * <p>
 * 失效时机：任务停止或自然完成后引擎 {@code doClose} 会经 {@code PDKIntegration.releaseAssociateId}
 * 销毁 connector（连接池/客户端随之关闭）并把节点从注册表移除——验证器（含停机/完成前
 * 持有的）此后不可用，需要验证器的断言必须在停止/完成前完成，或在重启后等新连接器就绪。
 */
public final class TaskNodeVerifiers {

	private TaskNodeVerifiers() {
	}

	/**
	 * 轮询等待任务指定节点的连接器就绪并返回其旁路验证器。
	 *
	 * @param taskId         任务 id（DAG id，TaskDto _id 的 hex）
	 * @param nodeId         DAG 节点 id（如 TaskDtoBuilder.SOURCE_NODE_ID）
	 * @param config         连接配置（MongoVerifier 需要其中的 database 名；可为 null）
	 * @param timeoutSeconds 超时秒数
	 */
	public static ConnectorVerifier awaitVerifier(String taskId, String nodeId, DataMap config, long timeoutSeconds) {
		long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;
		while (System.currentTimeMillis() < deadline) {
			ConnectorVerifier verifier = findVerifier(taskId, nodeId, config);
			if (verifier != null) {
				return verifier;
			}
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Interrupted while waiting connector verifier: task=" + taskId + " node=" + nodeId, e);
			}
		}
		throw new IllegalStateException("Timed out waiting connector verifier ready: task=" + taskId + " node=" + nodeId
				+ "（任务未启动或连接器未完成 init）");
	}

	/** 单次查找：节点连接器未注册或未完成 init 时返回 null */
	public static ConnectorVerifier findVerifier(String taskId, String nodeId, DataMap config) {
		ConnectorNode connectorNode = findConnectorNode(taskId, nodeId);
		if (connectorNode == null) {
			return null;
		}
		TapConnector connector = connectorNode.getConnector();
		return connector == null ? null : VerifierFactory.create(connector, config);
	}

	/** 在全局注册表中查找任务指定节点的 ConnectorNode（dagId=taskId 且 associateId 含 nodeId） */
	private static ConnectorNode findConnectorNode(String taskId, String nodeId) {
		for (ConnectorNode connectorNode : connectorNodes()) {
			if (connectorNode == null || !taskId.equals(connectorNode.getDagId())) {
				continue;
			}
			String associateId = connectorNode.getAssociateId();
			if (associateId != null && associateId.contains("_" + nodeId + "_")) {
				return connectorNode;
			}
		}
		return null;
	}

	/** 反射读取 ConnectorNodeService 私有成员 connectorNodeMap（Map<String, ConnectorNodeHolder>） */
	private static List<ConnectorNode> connectorNodes() {
		try {
			Field field = ConnectorNodeService.class.getDeclaredField("connectorNodeMap");
			field.setAccessible(true);
			Map<?, ?> map = (Map<?, ?>) field.get(ConnectorNodeService.getInstance());
			List<ConnectorNode> nodes = new ArrayList<>(map.size());
			for (Object holder : map.values()) {
				Method getConnectorNode = holder.getClass().getDeclaredMethod("getConnectorNode");
				getConnectorNode.setAccessible(true);
				nodes.add((ConnectorNode) getConnectorNode.invoke(holder));
			}
			return nodes;
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException("Read ConnectorNodeService.connectorNodeMap by reflection failed", e);
		}
	}
}
