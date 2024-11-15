package io.tapdata.services;

import com.tapdata.entity.Connections;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.log.EmptyLog;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.GetTableInfoFunction;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.service.PdkCountEntity;
import io.tapdata.service.PdkCountReadType;
import io.tapdata.service.skeleton.annotation.RemoteService;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2024-11-13 19:14
 **/
@RemoteService
public class PdkCountService {
	public PdkCountEntity count(String connectionId, String tableName, String readType) {
		PdkCountEntity pdkCountEntity = new PdkCountEntity();
		AtomicReference<ConnectorNode> connectorNode = new AtomicReference<>();
		AtomicReference<Connections> connections = new AtomicReference<>();
		try {
			ConnectorNodeService.getInstance().globalConnectorNode(connectionId, null, new EmptyLog(), (res, err) -> {
				if (null != err) {
					throw err;
				}
				connectorNode.set(ConnectorNodeService.getInstance().getConnectorNode(res.getAssociateId()));
				connections.set(res.getConnections());
			});
		} catch (Exception e) {
			return pdkCountEntity.failed(e);
		}
		if (null == connectorNode.get()) {
			return pdkCountEntity.failed(new RuntimeException("Get global connector node is null"));
		}
		ConnectorFunctions connectorFunctions = connectorNode.get().getConnectorFunctions();
		if (PdkCountReadType.info.name().equals(readType)) {
			GetTableInfoFunction getTableInfoFunction = connectorFunctions.getGetTableInfoFunction();
			if (null == getTableInfoFunction) {
				return pdkCountEntity.failed(new RuntimeException(String.format("Connector %s not support get table info function", connections.get().getPdkType())));
			}
			try {
				TableInfo tableInfo = getTableInfoFunction.getTableInfo(connectorNode.get().getConnectorContext(), tableName);
				return pdkCountEntity.success(tableInfo.getNumOfRows());
			} catch (Throwable e) {
				return new PdkCountEntity().failed(new RuntimeException(String.format("Call get table info function failed, table name: %s", tableName), e));
			}
		} else if (PdkCountReadType.table.name().equals(readType)) {
			BatchCountFunction batchCountFunction = connectorFunctions.getBatchCountFunction();
			if (null == batchCountFunction) {
				return pdkCountEntity.failed(new RuntimeException(String.format("Connector %s not support count table function", connections.get().getPdkType())));
			}
			try {
				long count = batchCountFunction.count(connectorNode.get().getConnectorContext(), new TapTable(tableName));
				return pdkCountEntity.success(count);
			} catch (Throwable e) {
				return new PdkCountEntity().failed(new RuntimeException(String.format("Call count table function failed, table name: %s", tableName), e));
			}
		} else {
			return pdkCountEntity.failed(new RuntimeException(String.format("Invalid read type, optional choice: %s", Arrays.stream(PdkCountReadType.values()).map(Enum::name).collect(Collectors.toList()))));
		}
	}
}
