package io.tapdata.websocket.handler;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.index.PdkIndexService;
import io.tapdata.flow.engine.V2.index.TwoDbRedline;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * P1-1 · 「查询索引」连接运维动作 handler（TAP-12057）。
 *
 * <p>TM 经 {@code MessageQueueService}/WebSocket 下发 {@code data.type = "queryIndexes"} 的
 * pipe 消息，{@code ManagementWebsocketHandler} 按类型派发到本 handler；本 handler 按连接即时构建
 * PDK {@link ConnectorNode}（{@link DropTableHandler} 同款生命周期），委派 {@link PdkIndexService}
 * 忠实读回目标表全部物理索引，<b>同步返回</b> {@link WebSocketEventResult}（由派发器回发给原发送方）。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：只经 PDK 连接器作用于<b>用户库</b>（按连接 config 构建），
 * 连接元信息经 {@code clientMongoOperator}（TM REST 代理）解析，绝不触碰 TM 的 {@code MongoTemplate}。</p>
 */
@EventHandlerAnnotation(type = "queryIndexes")
public class QueryIndexesHandler extends BaseEventHandler {

	private static final Logger logger = LogManager.getLogger(QueryIndexesHandler.class);
	private static final String TAG = QueryIndexesHandler.class.getSimpleName();

	static final String TABLE_NAME = "tableName";
	static final String CONNECTIONS = "connections";

	PdkIndexService pdkIndexService = new PdkIndexService();

	@Override
	public Object handle(Map event, SendMessage sendMessage) {
		if (MapUtils.isEmpty(event)) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.QUERY_INDEXES_RESULT, "Event data cannot be empty");
		}
		String tableName = String.valueOf(event.getOrDefault(TABLE_NAME, ""));
		if (StringUtils.isBlank(tableName) || "null".equals(tableName)) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.QUERY_INDEXES_RESULT, "tableName cannot be empty");
		}
		Object connObj = event.get(CONNECTIONS);
		if (!(connObj instanceof Map)) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.QUERY_INDEXES_RESULT, "connections cannot be empty");
		}
		Connections connections = JSONUtil.map2POJO((Map) connObj, Connections.class);
		try {
			List<TapIndex> indexes = queryIndexes(connections, tableName);
			logger.info("Query indexes done, connection: {}, table: {}, size: {}", connections.getName(), tableName, indexes.size());
			return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.QUERY_INDEXES_RESULT,
					new QueryIndexesResult(connections.getId(), tableName, indexes));
		} catch (Throwable t) {
			String msg = String.format("Query indexes failed, table: %s, message: %s", tableName, t.getMessage());
			logger.error(msg, t);
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.QUERY_INDEXES_RESULT, msg,
					t instanceof Exception ? (Exception) t : new RuntimeException(t));
		}
	}

	/**
	 * 按连接即时构建 PDK 连接器节点，读回目标表全部物理索引；节点生命周期（INIT/STOP/release）在此封口。
	 * 该封口触达 Hazelcast / PDK 包下载，故属集成范畴，单测以 spy 隔离（编排在 {@code QueryIndexesHandlerTest}）。
	 */
	protected List<TapIndex> queryIndexes(Connections connections, String tableName) throws Throwable {
		// P1-3 两库红线（ADR-0002）：目标解析到平台自有库 → 响亮失败，绝不在平台库上读/建服务型索引。
		TwoDbRedline.assertTargetIsUserDb(connections.getDatabase_uri(), platformMongoUri());
		DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
		String associateId = connections.getName() + "_" + System.currentTimeMillis();
		try {
			PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator,
					databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
			ConnectorNode connectorNode = PdkUtil.createNode(
					connections.getId(),
					databaseType,
					clientMongoOperator,
					associateId,
					connections.getConfig(),
					new PdkTableMap(TapTableUtil.getTapTableMapByNodeId(AutoInspectConstants.MODULE_NAME, connections.getId(), System.currentTimeMillis())),
					new PdkStateMap(String.format("%s_%s", AutoInspectConstants.MODULE_NAME, connections.getId()), HazelcastUtil.getInstance()),
					PdkStateMap.globalStateMap(HazelcastUtil.getInstance()),
					InstanceFactory.instance(LogFactory.class).getLog()
			);
			try {
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);
				return pdkIndexService.queryIndexes(connectorNode, new TapTable(tableName));
			} finally {
				try {
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
				} catch (Exception e) {
					logger.warn("Stop connector node failed, table: {}, message: {}", tableName, e.getMessage());
				}
			}
		} finally {
			PDKIntegration.releaseAssociateId(associateId);
		}
	}

	/** 平台自有库 uri（{@code TAPDATA_MONGO_URI}）；抽为 seam 便于两库红线用例注入。见 ADR-0002。 */
	protected String platformMongoUri() {
		return CommonUtils.getenv("TAPDATA_MONGO_URI");
	}

	/**
	 * 回发载荷：自描述（回带 connectionId + tableName），便于前端/TM 按 payload 关联响应
	 * （本 pipe 通道为 fire-and-forget、无 reqId 关联）。
	 */
	public static class QueryIndexesResult implements Serializable {
		private static final long serialVersionUID = 1L;
		private String connectionId;
		private String tableName;
		private List<TapIndex> indexes;

		public QueryIndexesResult() {
		}

		public QueryIndexesResult(String connectionId, String tableName, List<TapIndex> indexes) {
			this.connectionId = connectionId;
			this.tableName = tableName;
			this.indexes = indexes;
		}

		public String getConnectionId() {
			return connectionId;
		}

		public void setConnectionId(String connectionId) {
			this.connectionId = connectionId;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public List<TapIndex> getIndexes() {
			return indexes;
		}

		public void setIndexes(List<TapIndex> indexes) {
			this.indexes = indexes;
		}
	}
}
