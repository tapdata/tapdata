package io.tapdata.websocket.handler;

import com.tapdata.constant.*;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import io.tapdata.aspect.DropTableFuncAspect;
import io.tapdata.aspect.NewFieldFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.autoinspect.connector.PdkConnector;
import io.tapdata.common.SettingService;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TaskTargetProcessorExCode_15;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.dropTableEvent;

/**
 * @author zed
 * @Description 删除mdm的物理表
 * @create 2023-06-15 17:37
 **/
@EventHandlerAnnotation(type = "dropTable")
public class DropTableHandler implements WebSocketEventHandler {

	private final static Logger logger = LogManager.getLogger(DropTableHandler.class);


	private ClientMongoOperator clientMongoOperator;
	private SettingService settingService;

	/**
	 * 初始化handler方法
	 *
	 * @param clientMongoOperator 查询管理端数据
	 */
	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {

	}

	/**
	 * @param clientMongoOperator 查询管理端数据
	 * @param settingService      系统配置常量
	 */
	@Override
	public void initialize(ClientMongoOperator clientMongoOperator, SettingService settingService) {
		this.clientMongoOperator = clientMongoOperator;
		this.settingService = settingService;
	}

	/**
	 * 测试指定的连接
	 *
	 * @param event
	 * @param sendMessage
	 * @return
	 */
	@Override
	public Object handle(Map event, SendMessage sendMessage) {
		logger.info(String.format("drop table, event: %s", event));
		if (MapUtils.isEmpty(event)) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DROP_TABLE, "Event data cannot be empty");
		}
		String tableName = (String) event.getOrDefault("tableName", "");
		Map connectionMap = (Map) event.getOrDefault("connections", "");
		Connections connections = JSONUtil.map2POJO(connectionMap, Connections.class);

		DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
		long ts = System.currentTimeMillis();

		String associateId = connections.getName() + "_" + ts;
		try {
			PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator,
					databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
			String TAG = DropTableHandler.class.getSimpleName();
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

				AtomicReference<TapDropTableEvent> tapDropTableEvent = new AtomicReference<>();
				try {
					DropTableFunction dropTableFunction = connectorNode.getConnectorFunctions().getDropTableFunction();
					if (dropTableFunction != null) {
						tapDropTableEvent.set(dropTableEvent(tableName));

						PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TARGET_DROP_TABLE, () -> dropTableFunction.dropTable(connectorNode.getConnectorContext(), tapDropTableEvent.get()), TAG);
					}
				} catch (Throwable throwable) {
					throw new TapEventException(TaskTargetProcessorExCode_15.DROP_TABLE_FAILED, "Table name: " + tableName, throwable)
							.addEvent(tapDropTableEvent.get());
				}
			} finally {
				try {
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
				} catch (Exception e) {
					logger.error(" Stop error{}", e.getMessage());
				}
			}
		}finally {
			PDKIntegration.releaseAssociateId(associateId);
		}

		return null;
	}

}
