package io.tapdata.websocket.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.FileProperty;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.Schema;
import com.tapdata.validator.SchemaFactory;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.TapInterface;
import io.tapdata.common.ConverterUtil;
import io.tapdata.common.TapInterfaceUtil;
import io.tapdata.entity.LoadSchemaResult;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.schema.SchemaProxy;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;

/**
 * @author samuel
 * @Description
 * @create 2020-08-25 16:21
 **/
@EventHandlerAnnotation(type = "reloadSchema")
public class LoadSchemaEventHandler extends BaseEventHandler implements WebSocketEventHandler {

	private Logger logger = LogManager.getLogger(LoadSchemaEventHandler.class);

	public final static String LOAD_TABLES = "tables";

	/**
	 * 加载指定的模型
	 * {
	 * tables: [
	 * {
	 * connId: '', // 数据源id
	 * tableName: '', // 需要加载的表名
	 * fileProperty: {}, // 如果加载文件，则通过该内嵌对象，传递文件的参数
	 * schema: {} // 加载后的结果
	 * }, {...}
	 * ]
	 * }
	 *
	 * @param event
	 * @param sendMessage
	 * @return
	 */
	@Override
	public Object handle(Map event, SendMessage sendMessage) {
		logger.info("Starting load schema, event: " + event);
		if (MapUtils.isEmpty(event)) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.EXECUTE_LOAD_SCHEMA_RESULT, "Input data cannot be null");
		}
		if (!event.containsKey(LOAD_TABLES) || event.get(LOAD_TABLES) == null || !(event.get(LOAD_TABLES) instanceof List)) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.EXECUTE_LOAD_SCHEMA_RESULT,
					String.format("Event not contains %s, event: %s", LOAD_TABLES, event));
		}

		Tables tables;
		try {
			tables = JSONUtil.map2POJO(event, new TypeReference<Tables>() {
			});
		} catch (Exception e) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.EXECUTE_LOAD_SCHEMA_RESULT,
					String.format("Convert map to List<LoadSchemaEvent> error: %s", e.getMessage()));
		}

		if (tables == null || tables.isEmpty()) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.EXECUTE_LOAD_SCHEMA_RESULT,
					String.format("No table need to load, event: %s", event));
		}

		logger.info("Load schema field, load tables: {}", tables);

		Runnable runnable = () -> {
			Thread.currentThread().setName(String.format("LOAD-SCHEMA-%s", Instant.now()));
			try {
				List<LoadSchemaEvent> loadSchemaEvents = tables.getTables();

				Map<String, Connections> connectionsMap = new HashMap<>();

				for (LoadSchemaEvent loadTable : loadSchemaEvents) {
					String connId = loadTable.getConnId();
					if (connectionsMap.containsKey(connId)) {
						Connections connection = connectionsMap.get(connId);
						connection.setTable_filter(connection.getTable_filter() + "," + loadTable.getTableName());
					} else {
						Query query = new Query(Criteria.where("id").is(connId));
						query.fields().exclude("schema");
						List<Connections> connections = MongodbUtil.getConnections(query, null, clientMongoOperator, true);
						if (CollectionUtils.isNotEmpty(connections)) {
							Connections connection = connections.get(0);
							try {
								wrapConnection(loadTable, connection);
								connection.setLoadSchemaField(true);
								connection.setFile_schema(loadTable.getTableName());
							} catch (Exception e) {
								throw new Exception(String.format("wrap connection failed, msg: %s", e.getMessage()), e);
							}
							connectionsMap.put(connId, connection);
						}
					}
				}

				Map<String, List<?>> connIdTablesMap = new HashMap<>();
				for (Map.Entry<String, Connections> entry : connectionsMap.entrySet()) {
					String connId = entry.getKey();
					Connections connection = entry.getValue();
					Schema schema = null;

					if (SchemaFactory.canLoad(connection)) {
						schema = SchemaFactory.loadSchemaList(connection, true);
					} else if (StringUtils.isBlank(connection.getPdkType())) {
						TapInterface tapInterface = TapInterfaceUtil.getTapInterface(connection.getDatabase_type(), null);
						List<RelateDataBaseTable> relateDataBaseTables = new ArrayList<>();
						connection.setTableConsumer(table -> {
							if (StringUtils.isNotBlank(table.getTable_name())) {
								relateDataBaseTables.add(table);
							}
						});
						if (tapInterface != null) {
							LoadSchemaResult loadSchemaResult = tapInterface.loadSchema(connection);
							if (loadSchemaResult == null) {
								throw new RuntimeException(String.format("connection name: %s, database type: %s, unsupported to load schema field",
										connection.getName(), connection.getDatabase_type()));
							} else {
								if (StringUtils.isNotBlank(loadSchemaResult.getErrMessage())) {
									throw new RuntimeException(String.format("load schema fields failed, connection name: %s, class name: %s, message: %s",
											connection.getName(),
											tapInterface.getClass().getName(),
											loadSchemaResult.getErrMessage()));
								} else {
									schema = new Schema(loadSchemaResult.getSchema());
									if (CollectionUtils.isEmpty(schema.getTables())) {
										schema.setTables(relateDataBaseTables);
									} else {
										schema.getTables().addAll(relateDataBaseTables);
									}
								}
							}
						}
					} else {
						ConnectionNode connectionNode = null;
						try {
							List<TapTable> tapTables = new ArrayList<>();
							DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());
							connectionNode = PDKIntegration.createConnectionConnectorBuilder()
									.withConnectionConfig(DataMap.create(connection.getConfig()))
									.withGroup(databaseType.getGroup())
									.withPdkId(databaseType.getPdkId())
									.withAssociateId(connection.getName() + "_" + connection.getUser_id())
									.withVersion(databaseType.getVersion())
									.build();
							PDKInvocationMonitor.invoke(connectionNode, PDKMethod.INIT, connectionNode::connectorInit,
									LoadSchemaEventHandler.class.getSimpleName());
							LoadSchemaRunner.loadPdkSchema(
									connection,
									connectionNode,
									table -> {
										if (null == table) {
											return;
										}
										if (StringUtils.isNotBlank(table.getName())) {
											tapTables.add(table);
										}
									}
							);
							connIdTablesMap.put(connId, tapTables);
						} finally {
							if (null != connectionNode) PDKInvocationMonitor.invoke(connectionNode, PDKMethod.STOP
									, connectionNode::connectorStop, LoadSchemaEventHandler.class.getSimpleName());
							PDKIntegration.releaseAssociateId(connection.getName() + "_" + connection.getUser_id());
						}
					}

					if (schema != null) {
						ConverterUtil.schemaConvert(schema.getTables(), connection.getDatabase_type());
						if (CollectionUtils.isNotEmpty(schema.getTables())) {
							connIdTablesMap.put(connId, schema.getTables());
						}
					}
				}

				List<LoadSchemaEvent> loadTables = new ArrayList<>();

				Iterator<String> connIdIter = connIdTablesMap.keySet().iterator();
				while (connIdIter.hasNext()) {
					String connId = connIdIter.next();
					List<?> tableSchema = connIdTablesMap.get(connId);
					tableSchema.forEach(t -> {
						String tableName = "";
						if (t instanceof RelateDataBaseTable) {
							tableName = ((RelateDataBaseTable) t).getTable_name();
						} else if (t instanceof TapTable) {
							tableName = ((TapTable) t).getName();
						}
						loadTables.add(new LoadSchemaEvent(
								connId, tableName, t
						));
					});
				}

				if (CollectionUtils.isEmpty(loadTables)) {
					String err = "Cannot find any model";
					throw new Exception(err);
				} else {
					sendMessage.send(WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.EXECUTE_LOAD_SCHEMA_RESULT, loadTables));
					for (String connId : connIdTablesMap.keySet()) {
						// After load schema, clear schema proxy
						SchemaProxy.getSchemaProxy().clear(connId);
					}
				}
			} catch (Exception e) {
				String msg = String.format("Load schema failed, msg: %s", e.getMessage());
				try {
					sendMessage.send(WebSocketEventResult.handleFailed(WebSocketEventResult.Type.EXECUTE_LOAD_SCHEMA_RESULT, msg));
				} catch (IOException ioException) {
					logger.error(String.format("Send error load schema result to websocket failed, msg: %s, root exception: %s", ioException.getMessage(), msg), ioException);
				}
			}
		};
		Thread thread = new Thread(runnable);
		thread.start();

		return null;
	}

	private <T> void wrapConnection(LoadSchemaEvent<T> loadSchemaEvent, Connections connections) {
		if (null == loadSchemaEvent && null == connections) {
			return;
		}
		connections.setTable_filter(loadSchemaEvent.getTableName());
		// file
		if (loadSchemaEvent.getFileProperty() != null) {
			FileProperty fileProperty = loadSchemaEvent.getFileProperty();
			fileProperty.init();
			BeanUtils.copyProperties(fileProperty, connections);
			connections.setFileProperty(fileProperty);
		}
	}

	public static class Tables<T> implements Serializable {

		private static final long serialVersionUID = 592488555924471118L;

		private List<LoadSchemaEvent<T>> tables;

		public Tables() {
		}

		public Tables(List<LoadSchemaEvent<T>> tables) {
			this.tables = tables;
		}

		public List<LoadSchemaEvent<T>> getTables() {
			return tables;
		}

		public void setTables(List<LoadSchemaEvent<T>> tables) {
			this.tables = tables;
		}

		public boolean isEmpty() {
			return CollectionUtils.isEmpty(tables);
		}

		@Override
		public String toString() {
			return "Tables{" +
					"tables=" + tables +
					'}';
		}
	}

	public static class LoadSchemaEvent<T> implements Serializable {
		private static final long serialVersionUID = -4149136665620360724L;
		private String connId;
		private String tableName;
		private FileProperty fileProperty;
		private T schema;

		public LoadSchemaEvent(String connId, String tableName, T schema) {
			this.connId = connId;
			this.tableName = tableName;
			this.schema = schema;
		}

		public String getConnId() {
			return connId;
		}

		public void setConnId(String connId) {
			this.connId = connId;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public T getSchema() {
			return schema;
		}

		public void setSchema(T schema) {
			this.schema = schema;
		}

		public FileProperty getFileProperty() {
			return fileProperty;
		}

		public void setFileProperty(FileProperty fileProperty) {
			this.fileProperty = fileProperty;
		}

		@Override
		public String toString() {
			return "LoadSchemaEvent{" +
					"connId='" + connId + '\'' +
					", tableName='" + tableName + '\'' +
					", fileProperty=" + fileProperty +
					", schema=" + schema +
					'}';
		}
	}
}
