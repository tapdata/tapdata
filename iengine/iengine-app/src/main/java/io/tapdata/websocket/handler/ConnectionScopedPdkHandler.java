package io.tapdata.websocket.handler;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.index.TwoDbRedline;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 「连接级 PDK 动作」handler 的公共基类（TAP-12057）。
 *
 * <p>索引通道有一读一写两个连接运维动作——{@link QueryIndexesHandler}（读回物理索引）与
 * {@link CreateIndexHandler}（建索引）。两者都<b>不属于任何任务 DAG</b>：没有节点上下文，只有一个连接
 * 和一张表，因此都要按连接即时构建一个 PDK {@link ConnectorNode}、用完即弃。这段
 * 「红线 → 建节点 → INIT → 动作 → STOP → 释放 associateId」的编排原本在两个 handler 里各抄一份。</p>
 *
 * <p><b>收尾语义是这里的重点</b>，也是复制粘贴最容易抄漏的部分：动作失败也必须 STOP、STOP 自身失败只记
 * 一条 warn 不能把成功的动作倒打成失败、associateId 申请了就必须在 finally 里还。这几条都由
 * {@code ConnectionScopedPdkHandlerTest} 钉住。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：一切都只经 PDK 连接器作用于<b>用户库</b>；目标解析到平台自有库时在
 * <b>建节点之前</b>响亮失败，绝不触碰 TM 的 {@code MongoTemplate}。</p>
 */
public abstract class ConnectionScopedPdkHandler extends BaseEventHandler {

	private static final Logger log = LogManager.getLogger(ConnectionScopedPdkHandler.class);

	/** 在一个已 INIT 的连接器节点上执行的动作。 */
	@FunctionalInterface
	public interface PdkNodeAction<T> {
		T apply(ConnectorNode connectorNode) throws Throwable;
	}

	/** PDK 调用监控的日志标签；各 handler 返回自己的类名，保证日志归得了因。 */
	protected abstract String pdkTag();

	/**
	 * 按连接即时构建 PDK 节点、执行 {@code action}、并保证收尾。
	 *
	 * <p>顺序不可改：红线在最前（错库连节点都不该建），{@code releaseAssociateId} 在最外层 finally
	 * （节点没建成也要还），STOP 在动作的 finally 且自身异常只 warn 不外抛。</p>
	 *
	 * @param tableName 仅用于日志归因；目标表由各动作自己传给 PDK
	 */
	protected <T> T withConnectorNode(Connections connections, String tableName, PdkNodeAction<T> action) throws Throwable {
		// P1-3 两库红线（ADR-0002）：目标解析到平台自有库 → 响亮失败，绝不在平台库上读/建服务型索引。
		TwoDbRedline.assertTargetIsUserDb(connections.getDatabase_uri(), platformMongoUri());
		String associateId = associateId(connections);
		try {
			ConnectorNode connectorNode = buildNode(connections, associateId);
			try {
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, pdkTag());
				return action.apply(connectorNode);
			} finally {
				try {
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, pdkTag());
				} catch (Exception e) {
					// 收尾失败不改变动作的成败：动作已经做完了，这里再抛会把成功的部署报成失败。
					log.warn("Stop connector node failed, table: {}, message: {}", tableName, e.getMessage());
				}
			}
		} finally {
			PDKIntegration.releaseAssociateId(associateId);
		}
	}

	/** associateId = 连接名 + 时间戳，仅需进程内唯一。 */
	protected String associateId(Connections connections) {
		return connections.getName() + "_" + System.currentTimeMillis();
	}

	/**
	 * 建出可用的连接器节点（下载 PDK 包 → 建节点）。触达 Hazelcast 与 PDK 包下载，属<b>集成范畴</b>，
	 * 故单列为 seam：{@link #withConnectorNode} 的编排得以离线单测。
	 */
	protected ConnectorNode buildNode(Connections connections, String associateId) throws Throwable {
		DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
		PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator,
				databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
		return PdkUtil.createNode(
				connections.getId(),
				databaseType,
				clientMongoOperator,
				associateId,
				connections.getConfig(),
				new PdkTableMap(connectionScopedTableMap(connections)),
				new PdkStateMap(String.format("%s_%s", AutoInspectConstants.MODULE_NAME, connections.getId()), HazelcastUtil.getInstance()),
				PdkStateMap.globalStateMap(HazelcastUtil.getInstance()),
				InstanceFactory.instance(LogFactory.class).getLog()
		);
	}

	/**
	 * 建节点用的表映射（seam）。连接级动作<b>没有任务上下文</b>，故用<b>空表映射</b>（同
	 * {@code ScriptExecutorsManager}）：目标表由各动作以 {@code new TapTable(tableName)} 显式传给 PDK，
	 * 节点本身不需要预热的表结构。
	 *
	 * <p><b>切勿</b>改回 {@code TapTableUtil.getTapTableMapByNodeId(…, connections.getId(), …)}：那条路径查 TM 的
	 * {@code /MetadataInstances/node/tableMap}，只认<b>任务 DAG 节点 id</b>；传连接 id 会让 TM 在
	 * {@code taskDto} 为 null 上抛 SystemError，整个动作以 ERROR 收场（2026-07-30 实机验证所见）。</p>
	 */
	protected TapTableMap<String, TapTable> connectionScopedTableMap(Connections connections) {
		return TapTableMap.create(AutoInspectConstants.MODULE_NAME, connections.getId());
	}

	/** 平台自有库 uri（{@code TAPDATA_MONGO_URI}）；抽为 seam 便于两库红线用例注入。见 ADR-0002。 */
	protected String platformMongoUri() {
		return CommonUtils.getenv("TAPDATA_MONGO_URI");
	}
}
