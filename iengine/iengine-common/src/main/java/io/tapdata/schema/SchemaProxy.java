package io.tapdata.schema;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.dataflow.Capitalized;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.SyncObjects;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2021-11-09 19:10
 **/
public class SchemaProxy {

	private ConcurrentHashMap<String, SchemaMap> connectionSchemaMap = new ConcurrentHashMap<>();
	private ClientMongoOperator clientMongoOperator;
	public static SchemaProxy schemaProxy;

	public static SchemaProxy getSchemaProxy() {
		if (null == schemaProxy) {
			throw new NullPointerException();
		}
		return schemaProxy;
	}

	private SchemaProxy() {
	}

	/**
	 * 注册连接
	 *
	 * @param connectionId
	 * @param tableNames
	 */
	public void register(String connectionId, List<String> tableNames) {
		SchemaMap schemaMap;
		if (StringUtils.isBlank(connectionId)) {
			throw new IllegalArgumentException("Connection id cannot be emtpy");
		}
		Query query = new Query(Criteria.where("id").is(connectionId));
		query.fields().exclude("schema");
		Connections connections = clientMongoOperator.findOne(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
		if (null == connections) {
			throw new RuntimeException("Connection " + connectionId + " does not exists");
		}
		if (connectionSchemaMap.containsKey(connectionId)) {
			schemaMap = connectionSchemaMap.get(connectionId);
			schemaMap.getSchemaContext().setConnections(connections);
		} else {
			SchemaContext schemaContext = new SchemaContext(clientMongoOperator, connections);
			schemaMap = new SchemaMap(schemaContext);
			connectionSchemaMap.put(connectionId, schemaMap);
		}
		schemaMap.addTableNames(tableNames);
	}

	public void registerDataFlow(DataFlow dataFlow) {
		if (dataFlow == null) {
			return;
		}
		List<Stage> stages = dataFlow.getStages();
		Map<String, Set<String>> connectionIdTableName = new HashMap<>();

		for (Stage stage : stages) {
			if (!DataFlowStageUtil.isDataStage(stage.getType())) {
				continue;
			}
			String connectionId = stage.getConnectionId();
			if (StringUtils.isEmpty(connectionId)) {
				continue;
			}
			if (!connectionIdTableName.containsKey(connectionId)) {
				connectionIdTableName.put(connectionId, new HashSet<>());
			}
			Set<String> tableNames = connectionIdTableName.get(connectionId);
			Stage.StageTypeEnum stageTypeEnum = Stage.StageTypeEnum.fromString(stage.getType());
			switch (stageTypeEnum) {
				case DATABASE:
					if (stage.getSyncObjects() == null || CollectionUtils.isEmpty(stage.getSyncObjects())) {
						List<Stage> allDbSinkStages = findAllDbSinkStages(stage, stages);
						if (null == allDbSinkStages) {
							break;
						}
						for (Stage sinkStage : allDbSinkStages) {
							addDatabaseIncludeTableNames(sinkStage, tableNames, "source");
						}
					} else {
						addDatabaseIncludeTableNames(stage, tableNames, "sink");
						break;
					}
				default:
					String tableName = stage.getTableName();
					if (StringUtils.isNotBlank(tableName)) {
						tableNames.add(tableName);
					}
					break;
			}
		}

		Iterator<String> iterator = connectionIdTableName.keySet().iterator();
		while (iterator != null && iterator.hasNext()) {
			String connectionId = iterator.next();
			Set<String> tableNames = connectionIdTableName.get(connectionId);
			register(connectionId, new ArrayList<>(tableNames));
		}
	}

	/**
	 * Add table names
	 *
	 * @param stage
	 * @param tableNames
	 * @param type       source - not handle prefix/suffix/capitalized; sink - handle prefix/suffix/capitalized
	 */
	private void addDatabaseIncludeTableNames(Stage stage, Set<String> tableNames, String type) {
		SyncObjects syncObjects = stage.getSyncObjects().stream().filter(syncObject -> syncObject.getType().equals(SyncObjects.TABLE_TYPE)).findFirst().orElse(null);
		if (syncObjects == null) {
			return;
		}
		if (CollectionUtils.isEmpty(syncObjects.getObjectNames())) {
			return;
		}
		for (String objectName : syncObjects.getObjectNames()) {
			if (type.equals("sink")) {
				if (StringUtils.isNotBlank(stage.getTablePrefix()))
					objectName = stage.getTablePrefix() + objectName;
				if (StringUtils.isNotBlank(stage.getTableSuffix()))
					objectName = objectName + stage.getTableSuffix();
				objectName = Capitalized.convert(objectName, stage.getTableNameTransform());
			}
			tableNames.add(objectName);
		}
	}

	private List<Stage> findAllDbSinkStages(Stage sourceStage, List<Stage> stages) {
		List<String> outputLanes = sourceStage.getOutputLanes();
		if (CollectionUtils.isEmpty(outputLanes)) {
			return null;
		}

		return stages.stream().filter(stage -> outputLanes.contains(stage.getId())).collect(Collectors.toList());
	}

	public SchemaMap getSchemaMap(String connectionId) {
		return connectionSchemaMap.get(connectionId);
	}

	public void setClientMongoOperator(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	public enum SchemaProxyInstance {
		INSTANCE;

		private final SchemaProxy schemaProxy;

		SchemaProxyInstance() {
			schemaProxy = new SchemaProxy();
		}

		public SchemaProxy getInstance(ClientMongoOperator clientMongoOperator) {
			schemaProxy.setClientMongoOperator(clientMongoOperator);
			return schemaProxy;
		}
	}

	public void clear(String connectionId) {
		if (StringUtils.isBlank(connectionId) || !connectionSchemaMap.containsKey(connectionId)) {
			return;
		}
		SchemaMap schemaMap = connectionSchemaMap.get(connectionId);
		schemaMap.clear();
	}
}
