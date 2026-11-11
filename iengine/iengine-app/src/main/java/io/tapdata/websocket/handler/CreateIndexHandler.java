package io.tapdata.websocket.handler;

import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Connections;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * P3 · 「建索引」连接运维动作 handler（TAP-12057）。读侧 {@link QueryIndexesHandler} 的写向对偶。
 *
 * <p>TM 落地 service 经 {@code MessageQueueService}/WebSocket 下发 {@code data.type = "createIndex"}，
 * PDK {@link ConnectorNode} 的构建与生命周期由 {@link ConnectionScopedPdkHandler} 封口（与读侧
 * {@link QueryIndexesHandler} 共用），本 handler 逐条委派 {@link PdkIndexService#createIndex}，
 * <b>同步返回</b> {@link WebSocketEventResult}。</p>
 *
 * <p><b>本 handler 不做身份比对</b>（ADR-0005）：创建/跳过的判定由 TM 侧前置 {@code QueryIndexes} +
 * {@code ServingIndexLandingPlanner} 完成，下发到这里的<b>已经是「将创建」那一桶</b>。索引名也由 TM 按
 * 字段集确定性推导后带下来，这里照用不再生成。连接器把 errorCode 85/86 catch 后 continue（调用方收到"成功"）
 * ——那只是<b>第二道幂等兜底</b>，绝不能当判定手段。</p>
 *
 * <p><b>不首错即停</b>（P3-5）：一条索引建失败不中断其余，逐条记进 {@link CreateIndexResult#getCreated()} /
 * {@link CreateIndexResult#getFailed()}，由 TM 汇总进落地报告。整体性失败（连接解析、节点构建、红线）才
 * 走失败结果。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：只经 PDK 连接器作用于<b>用户库</b>，绝不触碰 TM 的 {@code MongoTemplate}；
 * 目标解析到平台自有库即在建节点前响亮失败。</p>
 */
@EventHandlerAnnotation(type = "createIndex")
public class CreateIndexHandler extends ConnectionScopedPdkHandler {

	private static final Logger logger = LogManager.getLogger(CreateIndexHandler.class);
	private static final String TAG = CreateIndexHandler.class.getSimpleName();

	static final String TABLE_NAME = "tableName";
	static final String CONNECTIONS = "connections";
	static final String REQ_ID = "reqId";
	static final String INDEXES = "indexes";
	static final String FIELDS = "fields";
	static final String FIELD = "field";
	static final String ASC = "asc";
	static final String NAME = "name";
	static final String UNIQUE = "unique";

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
		// reqId 同读侧（ADR-0009）：本 pipe 通道无内建请求-应答关联，先解析、任何失败路径都要带上它回发。
		String reqId = Objects.toString(event.get(REQ_ID), null);
		String tableName = String.valueOf(event.getOrDefault(TABLE_NAME, ""));
		if (StringUtils.isBlank(tableName) || "null".equals(tableName)) {
			return failed("tableName cannot be empty", null, null, reqId, null);
		}
		Object connObj = event.get(CONNECTIONS);
		if (!(connObj instanceof Map)) {
			return failed("connections cannot be empty", null, tableName, reqId, null);
		}
		List<TapIndex> indexes = toTapIndexes(event.get(INDEXES));
		if (indexes.isEmpty()) {
			// 响亮失败而非静默成功：调用方只在「将创建」桶非空时才该下发，空载荷是接线 bug。
			return failed("indexes cannot be empty", null, tableName, reqId, null);
		}
		Connections connections = JSONUtil.map2POJO((Map) connObj, Connections.class);
		CreateIndexResult payload = new CreateIndexResult(connections.getId(), tableName);
		payload.setReqId(reqId);
		try {
			createIndexes(connections, tableName, indexes, payload);
			logger.info("Create index done, connection: {}, table: {}, created: {}, failed: {}",
					connections.getName(), tableName, payload.getCreated().size(), payload.getFailed().size());
			return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.CREATE_INDEX_RESULT, payload);
		} catch (Throwable t) {
			String msg = String.format("Create index failed, table: %s, message: %s", tableName, t.getMessage());
			logger.error(msg, t);
			WebSocketEventResult result = WebSocketEventResult.handleFailed(
					WebSocketEventResult.Type.CREATE_INDEX_RESULT, msg,
					t instanceof Exception ? (Exception) t : new RuntimeException(t));
			result.setResult(payload);
			return result;
		}
	}

	/** 失败结果同样挂载荷（关联键 + 空清单）；理由同读侧：不带载荷的失败包调用方匹配不上、只能干等超时。 */
	private WebSocketEventResult failed(String error, String connectionId, String tableName, String reqId, Exception e) {
		WebSocketEventResult result = null == e
				? WebSocketEventResult.handleFailed(WebSocketEventResult.Type.CREATE_INDEX_RESULT, error)
				: WebSocketEventResult.handleFailed(WebSocketEventResult.Type.CREATE_INDEX_RESULT, error, e);
		CreateIndexResult payload = new CreateIndexResult(connectionId, tableName);
		payload.setReqId(reqId);
		result.setResult(payload);
		return result;
	}

	/** 线上载荷（{@code [{name, unique, fields:[{field, asc}]}]}）→ {@link TapIndex} 列表；非法形状一律忽略。 */
	static List<TapIndex> toTapIndexes(Object raw) {
		List<TapIndex> indexes = new ArrayList<>();
		if (!(raw instanceof List)) {
			return indexes;
		}
		for (Object item : (List<?>) raw) {
			if (item instanceof Map) {
				@SuppressWarnings("unchecked")
				TapIndex index = toTapIndex((Map<String, Object>) item);
				if (null != index) {
					indexes.add(index);
				}
			}
		}
		return indexes;
	}

	/**
	 * 单条载荷 → {@link TapIndex}。<b>方向必须忠实</b>：{@code asc == FALSE → fieldAsc=false}，
	 * {@code TRUE/缺省 → true}（同 P0 语义）——方向在这里丢掉，就是 p0 那个「加载读 -1 / 部署写 1」的翻版。
	 * 名字由 TM 按字段集确定性推导后带下来（§3.4），{@code unique} 仅作创建参数。
	 */
	static TapIndex toTapIndex(Map<String, Object> spec) {
		if (MapUtils.isEmpty(spec)) {
			return null;
		}
		TapIndex index = new TapIndex();
		index.setName(Objects.toString(spec.get(NAME), null));
		Object unique = spec.get(UNIQUE);
		if (unique instanceof Boolean) {
			index.setUnique((Boolean) unique);
		}
		List<TapIndexField> fields = new ArrayList<>();
		Object rawFields = spec.get(FIELDS);
		if (rawFields instanceof List) {
			for (Object item : (List<?>) rawFields) {
				if (!(item instanceof Map)) {
					continue;
				}
				Map<?, ?> field = (Map<?, ?>) item;
				TapIndexField indexField = new TapIndexField();
				indexField.setName(Objects.toString(field.get(FIELD), null));
				indexField.setFieldAsc(!Boolean.FALSE.equals(field.get(ASC)));
				fields.add(indexField);
			}
		}
		index.setIndexFields(fields);
		return index;
	}

	/**
	 * 逐条建索引并记账：<b>失败不中断后续</b>（P3-5「不首错即停」），成功记名、失败记名 + 原因。
	 * 创建动作以 {@link IndexCreator} 注入，故本方法与 PDK 节点解耦、可离线单测。
	 */
	static void applyEach(List<TapIndex> indexes, IndexCreator creator, CreateIndexResult result) {
		for (TapIndex index : indexes) {
			try {
				creator.create(index);
				result.getCreated().add(index.getName());
			} catch (Throwable t) {
				logger.warn("Create index failed, index: {}, message: {}", index.getName(), t.getMessage());
				result.getFailed().add(new FailedIndex(index.getName(), String.valueOf(t.getMessage())));
			}
		}
	}

	/**
	 * 逐条下发索引创建。节点构建与生命周期（红线/INIT/STOP/release）由
	 * {@link ConnectionScopedPdkHandler#withConnectorNode} 封口，本方法只提供「拿到节点之后做什么」。
	 * 该封口触达 Hazelcast / PDK 包下载，故属集成范畴，单测以 spy 隔离（逐条记账逻辑见 {@link #applyEach}）。
	 */
	protected void createIndexes(Connections connections, String tableName, List<TapIndex> indexes,
								 CreateIndexResult result) throws Throwable {
		withConnectorNode(connections, tableName, connectorNode -> {
			TapTable table = new TapTable(tableName);
			// 一条索引一个事件：失败可归因到具体索引，配合 applyEach 的逐条记账。
			applyEach(indexes, index -> pdkIndexService.createIndex(connectorNode, table,
					new TapCreateIndexEvent().indexList(Collections.singletonList(index))), result);
			return null;
		});
	}

	/** 单条索引的创建动作（注入点），便于把逐条记账与 PDK 节点解耦。 */
	@FunctionalInterface
	public interface IndexCreator {
		void create(TapIndex index) throws Throwable;
	}

	/**
	 * 回发载荷：自描述（回带 connectionId + tableName + reqId）+ 逐条结果。
	 *
	 * <p>{@code created}/{@code failed} 是<b>本次实际动作</b>的记账；「跳过」不在这里——那是 TM 侧比对的产物，
	 * 压根不会下发到引擎。</p>
	 */
	public static class CreateIndexResult implements Serializable {
		private static final long serialVersionUID = 1L;
		private String connectionId;
		private String tableName;
		private String reqId;
		private List<String> created = new ArrayList<>();
		private List<FailedIndex> failed = new ArrayList<>();

		public CreateIndexResult() {
		}

		public CreateIndexResult(String connectionId, String tableName) {
			this.connectionId = connectionId;
			this.tableName = tableName;
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

		public List<String> getCreated() {
			return created;
		}

		public void setCreated(List<String> created) {
			this.created = created;
		}

		public List<FailedIndex> getFailed() {
			return failed;
		}

		public void setFailed(List<FailedIndex> failed) {
			this.failed = failed;
		}
	}

	/** 单条建索引失败的记账项（索引名 + 原因），由 TM 汇总进落地报告。 */
	public static class FailedIndex implements Serializable {
		private static final long serialVersionUID = 1L;
		private String name;
		private String error;

		public FailedIndex() {
		}

		public FailedIndex(String name, String error) {
			this.name = name;
			this.error = error;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getError() {
			return error;
		}

		public void setError(String error) {
			this.error = error;
		}
	}
}
