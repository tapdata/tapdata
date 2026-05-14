package io.tapdata.services;

import com.google.common.collect.ImmutableMap;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.pdk.PDKUtils;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.service.skeleton.annotation.RemoteService;
import io.tapdata.services.util.ConnectionNodeUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RemoteService
public class DiscoverSchemaService {
	private final static String TAG = DiscoverSchemaService.class.getSimpleName();
	private final static int BATCH_SIZE = 20;

	public void discoverSchema(String connectionId, Map<String, Object> nodeConfig) {
		PDKUtils pdkUtils = InstanceFactory.instance(PDKUtils.class);
		if (pdkUtils == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "pdkUtils is null");

		if (connectionId == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "connectionId is null");
		Connections connections = HazelcastTaskService.taskService().getConnection(connectionId);
		if (connections == null)
			throw new CoreException(NetErrors.CONNECTIONS_NOT_FOUND, "Connections {} not found", connectionId);

		PDKUtils.PDKInfo pdkInfo = pdkUtils.downloadPdkFileIfNeed(connections.getPdkHash());

		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		LoadSchemaRunner loadSchemaRunner = new LoadSchemaRunner(connections, clientMongoOperator, 0, nodeConfig);
		loadSchemaRunner.run();
	}

	public MetadataInstancesDto discoverSpecifySchema(String connectionId, String tableName) {
		List<MetadataInstancesDto> metadataInstances = discoverSpecifySchemas(connectionId, List.of(tableName));
		if (CollectionUtils.isEmpty(metadataInstances)) {
			return null;
		}
		return metadataInstances.get(0);
	}

	public List<MetadataInstancesDto> discoverSpecifySchemas(String connectionId, List<String> tables) {
		if (CollectionUtils.isEmpty(tables)) {
			return new ArrayList<>();
		}
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		String associateId = "DiscoverSchema_" + connectionId + "_" + UUID.randomUUID();
		ConnectionNode connectionNode = null;
		try {
			connectionNode = ConnectionNodeUtil.createConnectionNode(clientMongoOperator, connectionId, associateId);
			DefaultExpressionMatchingMap dataTypesMap = connectionNode.getConnectionContext().getSpecification().getDataTypesMap();
			String schemaVersion = UUIDGenerator.uuid();
			Long lastUpdate = System.currentTimeMillis();
			PDKInvocationMonitor.invoke(connectionNode, PDKMethod.INIT, connectionNode::connectorInit, "Init PDK", TAG);
			ConnectionNode finalConnectionNode = connectionNode;
			int batchSize = Math.min(tables.size(), BATCH_SIZE);
			PDKInvocationMonitor.invoke(connectionNode, PDKMethod.DISCOVER_SCHEMA,
					() -> {
						finalConnectionNode.getConnector().discoverSchema(finalConnectionNode.getConnectionContext(), tables, batchSize,
								tapTables -> {
									if (CollectionUtils.isEmpty(tapTables)) {
										return;
									}
									Update update = new Update();
									TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
									for (TapTable pdkTable : tapTables) {
										LinkedHashMap<String, TapField> nameFieldMap = pdkTable.getNameFieldMap();
										if (MapUtils.isNotEmpty(nameFieldMap)) {
											nameFieldMap.forEach((fieldName, tapField) -> {
												if (null == tapField.getTapType()) {
													tableFieldTypesGenerator.autoFill(tapField, dataTypesMap);
												}
											});
										}
									}
									update.set("schema.tables", tapTables);
									update.set(ConnectorConstant.LOAD_FIELDS, ConnectorConstant.LOAD_FIELD_STATUS_FINISHED)
											.set(DataSourceConnectionDto.FIELD_SCHEMA_VERSION, schemaVersion)
											.set(DataSourceConnectionDto.FIELD_LAST_UPDATE, lastUpdate)
											.set(DataSourceConnectionDto.FIELD_EVER_LOAD_SCHEMA, true);
									Query query = new Query(Criteria.where("_id").is(connectionId));
									clientMongoOperator.update(query, update, ConnectorConstant.CONNECTION_COLLECTION + "/module");
								});
					}, TAG);
			Map<String, Object> where = new HashMap<>();
			where.put("source._id", connectionId);
			where.put("original_name", ImmutableMap.of("$in", tables));
			where.put("sourceType", SourceTypeEnum.SOURCE.name());
			where.put("is_deleted", ImmutableMap.of("$ne", true));
            return clientMongoOperator.find(where, ConnectorConstant.METADATA_INSTANCE_COLLECTION, MetadataInstancesDto.class);
		} catch (Throwable e) {
			throw new CoreException(NetErrors.ILLEGAL_STATE, e, "Load schema failed, schema: {}, error: {}", TapSimplify.toJson(tables), e.getMessage());
		} finally {
			if (connectionNode != null)
				PDKInvocationMonitor.invoke(connectionNode, PDKMethod.STOP, connectionNode::connectorStop, "Stop PDK", TAG);
			PDKIntegration.releaseAssociateId(associateId);
		}
	}

}
