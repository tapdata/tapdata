package io.tapdata.services;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.entity.EmptyMap;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.EmptyTapTableMap;
import io.tapdata.service.skeleton.annotation.RemoteService;
import java.util.Map;
import java.util.UUID;
import com.tapdata.tm.commons.inspect.CustomQueryConfig;
import com.tapdata.tm.commons.inspect.CustomQueryRequest;
import io.tapdata.inspect.CustomQueryRunner;
import org.apache.commons.collections4.MapUtils;

@RemoteService
public class CustomQueryPreviewService {
    public CustomQueryConfig.Snapshot test(CustomQueryRequest request) {
        request.validate();
        String associateId = "inspect_preview_" + UUID.randomUUID();
        ConnectorNode node = null;
        boolean cleanupTransferred = false;
        try {
            ClientMongoOperator mongo = BeanUtil.getBean(ClientMongoOperator.class);
            Connections connection = HazelcastTaskService.taskService().getConnection(request.getConnectionId());
            if (connection == null) throw new IllegalArgumentException("Connection is unavailable");
            DatabaseTypeEnum.DatabaseType type = ConnectionUtil.getDatabaseType(mongo, connection.getPdkHash());
            resolveTimezone(request, connection);
            if (!(mongo instanceof HttpClientMongoOperator))
                throw new IllegalStateException("Custom query preview requires the HTTP Mongo operator");
            node = createConnectorNode(associateId, (HttpClientMongoOperator) mongo, type, connection.getConfig());
            PDKInvocationMonitor.invoke(node, PDKMethod.INIT, node::connectorInit, getClass().getSimpleName());
            ConnectorNode previewNode = node;
            cleanupTransferred = true;
            CustomQueryConfig.Snapshot snapshot = CustomQueryRunner.execute(node, request, 20, true,
                    new CustomQueryConfig.SideSummary(), () -> release(previewNode, associateId));
            if (snapshot.getColumns().isEmpty()) throw new IllegalArgumentException("This empty query result has no discoverable columns; test with representative data or use a connector with result metadata support");
            snapshot.setTimezone(request.getTimezone());
            for (Map<String, Object> row : snapshot.getRows()) row.replaceAll((key, value) ->
                    value instanceof java.math.BigDecimal || value instanceof java.math.BigInteger || value instanceof Long
                            ? value.toString() : value);
            CustomQueryConfig schemaConfig = new CustomQueryConfig();
            schemaConfig.setIgnoreCase(request.isIgnoreCase()); schemaConfig.columnNames(snapshot);
            return snapshot;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Custom query test failed: " + e.getMessage(), e);
        } finally {
            if (!cleanupTransferred) release(node, associateId);
        }
    }
    static void resolveTimezone(CustomQueryRequest request, Connections connection) {
        if (request.getTimezone() == null || request.getTimezone().isBlank()) {
            Object timezone = connection.getConfig() == null ? null : connection.getConfig().get("timezone");
            request.setTimezone(timezone == null || timezone.toString().isBlank() ? "UTC" : timezone.toString());
        }
    }
    private void release(ConnectorNode node, String associateId) {
        try {
            if (node != null) PDKInvocationMonitor.invoke(node, PDKMethod.STOP, node::connectorStop, getClass().getSimpleName());
        } catch (Exception ignored) {
            // Release the association even if the connector cannot stop cleanly.
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException("Cannot stop custom query connector", e);
        } finally {
            PDKIntegration.releaseAssociateId(associateId);
        }
    }
	private ConnectorNode createConnectorNode(String associateId, HttpClientMongoOperator clientMongoOperator,
											  DatabaseTypeEnum.DatabaseType databaseType,
											  Map<String, Object> connectionConfig) {
		try {
			PdkUtil.downloadPdkFileIfNeed(clientMongoOperator,
					databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
			PDKIntegration.ConnectorBuilder<ConnectorNode> connectorBuilder = PDKIntegration.createConnectorBuilder()
					.withDagId(associateId)
					.withAssociateId(associateId)
					.withConfigContext(null)
					.withGroup(databaseType.getGroup())
					.withVersion(databaseType.getVersion())
					.withPdkId(databaseType.getPdkId())
					.withTableMap(new EmptyTapTableMap())
					.withStateMap(new EmptyMap())
					.withGlobalStateMap(new EmptyMap());
			if (MapUtils.isNotEmpty(connectionConfig)) {
				connectorBuilder.withConnectionConfig(DataMap.create(connectionConfig));
			}
			return connectorBuilder.build();
		} catch (Exception e) {
			throw new RuntimeException("Failed to create custom query connector node, database type: " + databaseType + ", message: " + e.getMessage(), e);
		}
	}

}
