package io.tapdata.websocket.handler;

import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Connections;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.index.PdkIndexService;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * P1-1 · 「查询索引」连接运维动作 handler（TAP-12057）。
 *
 * <p>TM 经 {@code MessageQueueService}/WebSocket 下发 {@code data.type = "queryIndexes"} 的
 * pipe 消息，{@code ManagementWebsocketHandler} 按类型派发到本 handler；PDK {@link ConnectorNode} 的
 * 构建与生命周期由 {@link ConnectionScopedPdkHandler} 封口（与写侧 {@link CreateIndexHandler} 共用），
 * 本 handler 只负责事件解析 + 委派 {@link PdkIndexService} 忠实读回目标表全部物理索引，
 * <b>同步返回</b> {@link WebSocketEventResult}（由派发器回发给原发送方）。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：只经 PDK 连接器作用于<b>用户库</b>（按连接 config 构建），
 * 连接元信息经 {@code clientMongoOperator}（TM REST 代理）解析，绝不触碰 TM 的 {@code MongoTemplate}。</p>
 */
@EventHandlerAnnotation(type = "queryIndexes")
public class QueryIndexesHandler extends ConnectionScopedPdkHandler {

	private static final Logger logger = LogManager.getLogger(QueryIndexesHandler.class);
	private static final String TAG = QueryIndexesHandler.class.getSimpleName();

	static final String TABLE_NAME = "tableName";
	static final String CONNECTIONS = "connections";
	static final String REQ_ID = "reqId";

	PdkIndexService pdkIndexService = new PdkIndexService();

	@Override
	protected String pdkTag() {
		return TAG;
	}

	@Override
	public Object handle(Map event, SendMessage sendMessage) {
		if (MapUtils.isEmpty(event)) {
			return failed("Event data cannot be empty", null, null, null, null);
		}
		// reqId：前端生成、经 pipe 事件透传，在结果里回显作为关联键（本 pipe 通道无内建请求-应答关联）。见 ADR-0009。
		// 先解析：**任何**失败路径都要带上它回发，否则前端匹配不上、只能干等超时。
		String reqId = Objects.toString(event.get(REQ_ID), null);
		String tableName = String.valueOf(event.getOrDefault(TABLE_NAME, ""));
		if (StringUtils.isBlank(tableName) || "null".equals(tableName)) {
			return failed("tableName cannot be empty", null, null, reqId, null);
		}
		Object connObj = event.get(CONNECTIONS);
		if (!(connObj instanceof Map)) {
			return failed("connections cannot be empty", null, tableName, reqId, null);
		}
		Connections connections = JSONUtil.map2POJO((Map) connObj, Connections.class);
		try {
			List<TapIndex> indexes = queryIndexes(connections, tableName);
			logger.info("Query indexes done, connection: {}, table: {}, size: {}", connections.getName(), tableName, indexes.size());
			QueryIndexesResult payload = new QueryIndexesResult(connections.getId(), tableName, indexes);
			payload.setReqId(reqId);
			return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.QUERY_INDEXES_RESULT, payload);
		} catch (Throwable t) {
			String msg = String.format("Query indexes failed, table: %s, message: %s", tableName, t.getMessage());
			logger.error(msg, t);
			return failed(msg, connections.getId(), tableName, reqId,
					t instanceof Exception ? (Exception) t : new RuntimeException(t));
		}
	}

	/**
	 * 失败结果同样挂 {@link QueryIndexesResult} 载荷（关联键 + 空索引列表）。
	 *
	 * <p>{@code WebSocketEventResult.handleFailed} 只填 {@code error}/{@code status}、{@code result} 为 null；
	 * 而前端按 {@code reqId}（连同 connectionId + tableName）关联在飞请求——不带载荷的失败包会被前端直接忽略，
	 * 表现为「点了加载没反应、60s 后超时」而不是即时报错（2026-07-30 实机验证所见）。</p>
	 */
	private WebSocketEventResult failed(String error, String connectionId, String tableName, String reqId, Exception e) {
		WebSocketEventResult result = null == e
				? WebSocketEventResult.handleFailed(WebSocketEventResult.Type.QUERY_INDEXES_RESULT, error)
				: WebSocketEventResult.handleFailed(WebSocketEventResult.Type.QUERY_INDEXES_RESULT, error, e);
		QueryIndexesResult payload = new QueryIndexesResult(connectionId, tableName, Collections.emptyList());
		payload.setReqId(reqId);
		result.setResult(payload);
		return result;
	}

	/**
	 * 读回目标表全部物理索引。节点构建与生命周期（红线/INIT/STOP/release）由
	 * {@link ConnectionScopedPdkHandler#withConnectorNode} 封口，本方法只提供「拿到节点之后做什么」。
	 * 该封口触达 Hazelcast / PDK 包下载，故属集成范畴，单测以 spy 隔离（编排在 {@code QueryIndexesHandlerTest}）。
	 */
	protected List<TapIndex> queryIndexes(Connections connections, String tableName) throws Throwable {
		return withConnectorNode(connections, tableName,
				connectorNode -> pdkIndexService.queryIndexes(connectorNode, new TapTable(tableName)));
	}

	/**
	 * 回发载荷：自描述（回带 connectionId + tableName + reqId），便于前端/TM 按 payload 关联响应。
	 *
	 * <p>本 pipe 通道 fire-and-forget、无内建请求-应答关联；{@code reqId}（前端生成、经事件透传、在此回显）
	 * 是唯一的关联键——两个在飞的同 (连接, 表) 查询仅凭 connectionId+tableName 无法区分。见 ADR-0009。</p>
	 */
	public static class QueryIndexesResult implements Serializable {
		private static final long serialVersionUID = 1L;
		private String connectionId;
		private String tableName;
		private String reqId;
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

		public String getReqId() {
			return reqId;
		}

		public void setReqId(String reqId) {
			this.reqId = reqId;
		}

		public List<TapIndex> getIndexes() {
			return indexes;
		}

		public void setIndexes(List<TapIndex> indexes) {
			this.indexes = indexes;
		}
	}
}
