package io.tapdata.flow.engine.V2.index;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryIndexesFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;

import java.util.ArrayList;
import java.util.List;

/**
 * P1-1 · 引擎侧可复用的索引读写通道（TAP-12057）。
 *
 * <p>加载（p2·读）与落地（p3·写）共用本通道：给定一个已构建的 PDK {@link ConnectorNode} 与目标
 * {@link TapTable}，忠实地读回全部物理索引 / 忠实地下发单个索引创建。构建 {@code ConnectorNode}
 * 本身（下载 PDK 包、Hazelcast 状态）由调用方负责（引擎目标节点复用其运行态节点；
 * 连接运维动作 handler 按 {@code DropTableHandler} 模式即时构建），故本服务保持无状态、可单测。</p>
 *
 * <p><b>刻意不做身份/去重比对</b>（见 <a href="../../../../../../../../adr/0005-conflict-add-only.md">ADR-0005</a>）：
 * {@link #queryIndexes} 返回连接器给出的<b>全部</b>物理索引，不按名、不按字段做任何过滤；
 * 「有序字段 + 方向」的身份比对属于 p3 落地层，绝不在此复用
 * {@code HazelcastTargetPdkDataNode#tapIndexEquals}（它按名短路）。</p>
 *
 * <p><b>两库红线</b>（见 <a href="../../../../../../../../adr/0002-two-db-redline-conmap.md">ADR-0002</a>）：
 * 本服务只经 PDK 连接器作用于<b>用户库</b>，绝不触碰 TM 的 {@code MongoTemplate}。</p>
 */
public class PdkIndexService {

	private static final String TAG = PdkIndexService.class.getSimpleName();

	/**
	 * 读回目标表在用户库中的<b>全部</b>物理索引，忠实返回、不做任何身份过滤。
	 *
	 * @param connectorNode 已初始化的 PDK 连接器节点（调用方负责其生命周期）
	 * @param table         目标表
	 * @return 连接器读出的全部索引；连接器不支持查询时返回空列表（不抛异常）
	 */
	public List<TapIndex> queryIndexes(ConnectorNode connectorNode, TapTable table) throws Throwable {
		QueryIndexesFunction queryIndexesFunction = connectorNode.getConnectorFunctions().getQueryIndexesFunction();
		List<TapIndex> indexes = new ArrayList<>();
		if (null == queryIndexesFunction) {
			return indexes;
		}
		queryIndexesFunction.query(connectorNode.getConnectorContext(), table, tapIndexList -> {
			if (null != tapIndexList) {
				indexes.addAll(tapIndexList);
			}
		});
		return indexes;
	}

	/**
	 * 在用户库中忠实创建给定索引（方向由连接器遵循 {@code TapIndexField.fieldAsc}，见 p0 修复）。
	 *
	 * @param connectorNode    已初始化的 PDK 连接器节点
	 * @param table            目标表
	 * @param createIndexEvent 索引创建事件（含有序字段 + 方向 + unique）
	 * @throws UnsupportedOperationException 连接器不支持创建索引时——显式失败，不静默跳过
	 */
	public void createIndex(ConnectorNode connectorNode, TapTable table, TapCreateIndexEvent createIndexEvent) {
		CreateIndexFunction createIndexFunction = connectorNode.getConnectorFunctions().getCreateIndexFunction();
		if (null == createIndexFunction) {
			throw new UnsupportedOperationException("Connector does not support create index");
		}
		PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TARGET_CREATE_INDEX,
				() -> createIndexFunction.createIndex(connectorNode.getConnectorContext(), table, createIndexEvent), TAG);
	}
}
