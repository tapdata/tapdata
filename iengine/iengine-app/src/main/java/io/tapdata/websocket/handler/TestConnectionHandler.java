package io.tapdata.websocket.handler;

import com.google.common.collect.Maps;
import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.validator.ConnectionValidateResult;
import com.tapdata.validator.ConnectionValidateResultDetail;
import com.tapdata.validator.ConnectionValidator;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.common.SettingService;
import io.tapdata.entity.BaseConnectionValidateResult;
import io.tapdata.exception.ConnectionException;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.IOException;
import java.util.*;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2020-12-22 15:39
 **/
@EventHandlerAnnotation(type = "testConnection")
public class TestConnectionHandler implements WebSocketEventHandler {

	private final static Logger logger = LogManager.getLogger(TestConnectionHandler.class);

	private final static String NOT_CHANGE_LAST_COLLECTION = ConnectorConstant.CONNECTION_COLLECTION;

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

		logger.info(String.format("Test connection, event: %s", event));
		if (MapUtils.isEmpty(event)) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_CONNECTION_RESULT, "Event data cannot be empty");
		}

		Runnable runnable = () -> {
			Thread.currentThread().setName(String.format("TEST-CONNECTION-%s", event.getOrDefault("name", "")));
			Connections connection = null;
			String connName = event.getOrDefault("name", "").toString();
			String schemaVersion = UUIDGenerator.uuid();
			Schema schema;

			try {
				try {
					if (event.containsKey("schema") && !(event.get("schema") instanceof Map)) {
						event.remove("schema");
					}
					connection = JSONUtil.map2POJO(event, Connections.class);
					connection.setPdkHash((String) event.getOrDefault("pdkHash", ""));
				} catch (Exception e) {
					String errMsg = String.format("Map convert to Connections failed, event: %s, err: %s", event, e.getMessage());
					logger.error(errMsg, e);
					sendMessage.send(WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_CONNECTION_RESULT,
							errMsg, e));
					return;
				}

				if (null == connection) {
					String errMsg = "Connections is null";
					logger.error(errMsg);
					sendMessage.send(WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_CONNECTION_RESULT, errMsg));
					return;
				}

				logger.info(TapLog.CON_LOG_0021.getMsg(), connection.getName());

				String id = connection.getId();
				Query connectionIdQuery = null;
				boolean save = false;
				boolean updateSchema = true;
				boolean everLoadSchema = false;
				boolean editTest = false;

				if (event.containsKey("updateSchema")) {
					try {
						updateSchema = Boolean.valueOf(event.get("updateSchema").toString());
					} catch (Exception ignore) {
					}
				}

				if (event.containsKey("everLoadSchema")) {
					try {
						everLoadSchema = Boolean.valueOf(event.get("everLoadSchema").toString());
					} catch (Exception ignore) {
					}
				}
				DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(connection.getDatabase_type());
				switch (databaseTypeEnum) {
					case FILE:
						updateSchema = false;
						everLoadSchema = true;
						break;
					default:
						break;
				}

				if (event.containsKey("editTest")) {
					try {
						editTest = Boolean.valueOf(event.get("editTest").toString());
					} catch (Exception ignore) {
					}
				}

				if (StringUtils.isNotBlank(id) && !editTest) {
					connectionIdQuery = new Query(Criteria.where("_id").is(id));
					connectionIdQuery.fields().exclude("schema");
					Connections newConnection = clientMongoOperator.findOne(connectionIdQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
					if(newConnection != null) {
						connection = newConnection;
						connection.setExtParam((Map) event.getOrDefault("extParam", Maps.newHashMap()));
						save = true;
					}
				}

				// Decode passwords
				connection.decodeDatabasePassword();
				if (StringUtils.isBlank(connection.getDatabase_password()) && event.containsKey("plain_password")) {
					Object plainPassword = event.get("plain_password");
					connection.setDatabase_password(plainPassword != null ? plainPassword.toString() : "");
				}

				// PDK connection test
				if (StringUtils.isBlank(connection.getPdkType())) {
					throw new ConnectionException("Unknown connection pdk type");
				}
				ConnectionValidateResult validateResult;
				DatabaseTypeEnum.DatabaseType databaseDefinition = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());
				if (databaseDefinition == null) {
					throw new ConnectionException(String.format("Unknown database type %s", connection.getDatabase_type()));
				}
				PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator, databaseDefinition.getPdkHash(), databaseDefinition.getJarFile(), databaseDefinition.getJarRid());
				validateResult = ConnectionValidator.testPdkConnection(connection, databaseDefinition);
				schema = validateResult.getSchema();
				List<ConnectionValidateResultDetail> validateResultDetails = validateResult.getValidateResultDetails();
				// Websocket returns test item(s)
				sendMessage.send(WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.TEST_CONNECTION_RESULT,
						wrapConnectionResult(connection, validateResult)));
				if (save) {
					// Update connection test result
					updatePDKConnectionTestResult(connectionIdQuery, updateSchema, everLoadSchema, validateResult, validateResultDetails);
				}

				if (needLoadField(updateSchema, schema, everLoadSchema) && save) {
					// Load schema field
					LoadSchemaRunner loadSchemaRunner = new LoadSchemaRunner(connection, clientMongoOperator,
							null == schema.getTables() ? schema.getTapTableCount() : schema.getTables().size());
					loadSchemaRunner.run();
				} else {
					updateLoadSchemaFieldStatus(connection, ConnectorConstant.LOAD_FIELD_STATUS_FINISHED, schema, schemaVersion);
				}
			} catch (Exception e) {
				Optional.of(event).ifPresent(map -> map.remove("config"));
				String errMsg = String.format("Test connection %s failed, data: %s, err: %s", connName, event, e.getMessage());
				logger.error(errMsg, e);
				try {
					sendMessage.send(WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_CONNECTION_RESULT, errMsg, e));
				} catch (IOException ioException) {
					logger.error(String.format("Send error test connection result to websocket failed, msg: %s, root exception: %s", ioException.getMessage(), errMsg), ioException);
				}
				if (connection != null) {
					Update update = getValidateResultUpdate(0, 0L, BaseConnectionValidateResult.CONNECTION_STATUS_INVALID,
							null, null, null, null);
					Query updateQuery = new Query(where("_id").is(connection.getId()));
					clientMongoOperator.update(updateQuery, update, NOT_CHANGE_LAST_COLLECTION);
				}
			}
		};

		Thread thread = new Thread(runnable);
		thread.start();

		return null;
	}

	private void updatePDKConnectionTestResult(Query connectionIdQuery, boolean updateSchema, boolean everLoadSchema, ConnectionValidateResult validateResult, List<ConnectionValidateResultDetail> validateResultDetails) {
		Update update = new Update();
		// Update connection
		update.set("response_body.retry", validateResult.getRetry());
		if (validateResult.getNextRetry() != null) {
			update.set("response_body.next_retry", validateResult.getNextRetry());
		}
		update.set("response_body.validate_details", validateResultDetails);
		update.set("status", validateResult.getStatus());
		if (needLoadField(updateSchema, validateResult.getSchema(), everLoadSchema)) {
			update.set("loadSchemaField", true);
		} else {
			update.set("loadSchemaField", false);
		}
		clientMongoOperator.update(connectionIdQuery, update, NOT_CHANGE_LAST_COLLECTION);
		// Update connection options
		update = new Update().set("options", validateResult.getConnectionOptions());
		clientMongoOperator.update(connectionIdQuery, update, ConnectorConstant.CONNECTION_COLLECTION + "/connectionOptions");
	}

	@Deprecated
	private void updateConnectionUniqueName(Connections connection, Query connectionIdQuery) {
		connection.setUniqueName(connection.getId());
		clientMongoOperator.update(connectionIdQuery, new Update().set("uniqueName", connection.getUniqueName()), NOT_CHANGE_LAST_COLLECTION);
	}

	private void updateLoadSchemaFieldStatus(Connections connection, String status, Schema schema, String schemaVersion) {
		Query query = new Query(Criteria.where("_id").is(connection.getId()));
		Update update = new Update().set(ConnectorConstant.LOAD_FIELDS, status);
		if (null != schema && schema.isIncludeFields()) {
			update.set("schemaVersion", schemaVersion);
		}
		clientMongoOperator.update(query, update, NOT_CHANGE_LAST_COLLECTION);
	}

	@Deprecated
	private void updateLoadSchemaStatus(Query connectionIdQuery, boolean updateSchema, boolean everLoadSchema, String schemaVersion, int schemaTablesSize, Schema schema
			, List<RelateDataBaseTable> schemaTables, List validateResultDetails, int retry, Long nextRetry, String status, Integer dbVersion, String dbFullVersion, Connections connections) {
		Update update = new Update();
		update.set("response_body.retry", retry);
		if (nextRetry != null) {
			update.set("response_body.next_retry", nextRetry);
		}
		update.set("response_body.validate_details", validateResultDetails);
		update.set("status", status);
		if (dbVersion != null) {
			update.set("db_version", dbVersion);
		}
		if (dbFullVersion != null) {
			update.set("dbFullVersion", dbFullVersion);
		}
		update.set("schemaVersion", schemaVersion);

		if (needLoadField(updateSchema, schema, everLoadSchema)) {
			update.set("loadSchemaField", true);
		} else {
			update.set("loadSchemaField", false);
		}

		if (StringUtils.isNotBlank(connections.getUniqueName())) {
			update.set("uniqueName", connections.getUniqueName());
		}

		// 分批提交结构，防止一次提交数据太多
		int BATCH_SIZE = 100, saveSchemaSize = 0;
		if (CollectionUtils.isNotEmpty(schemaTables)) {
			saveSchemaSize = schemaTables.size();
			update.set("schema.tables", schemaTables);
		}
		if (saveSchemaSize > BATCH_SIZE) {
			List<RelateDataBaseTable> tmpList = new ArrayList<>();
			for (int i = 0; i < saveSchemaSize; i++) {
				tmpList.add(schemaTables.get(i));
				if (i != 0 && i % BATCH_SIZE == 0) {
					update.set("schema.tables", tmpList);
					BATCH_SIZE = IRateTimeout.toSize(30000L, BATCH_SIZE, 500, 20, 0.3f, 0.2f, args -> {
						clientMongoOperator.update(connectionIdQuery, update, ConnectorConstant.CONNECTION_COLLECTION);
					}, null);
					tmpList.clear();
				}
			}
			if (!tmpList.isEmpty()) {
				update.set("schema.tables", tmpList);
				clientMongoOperator.update(connectionIdQuery, update, NOT_CHANGE_LAST_COLLECTION);
			}
		} else {
			clientMongoOperator.update(connectionIdQuery, update, NOT_CHANGE_LAST_COLLECTION);
		}
	}

	private boolean needLoadField(boolean updateSchema, Schema schema, boolean everLoadSchema) {
		if (schema == null) {
			return false;
		}

		if (schema.isIncludeFields()) {
			return false;
		}

		// 如果从来没加载过的数据源，强制加载字段模型
		if (!everLoadSchema) {
			return true;
		}

		if (!updateSchema) {
			return false;
		}

		return true;
	}

	@Deprecated
	private void setFileDefaultCharset(Connections connection) {

		Setting setting = settingService.getSetting("file.defaultCharset");
		if (setting != null) {
			connection.setFileDefaultCharset(setting.getValue());
		}
	}

	private Map<String, Object> wrapConnectionResult(Connections connection, ConnectionValidateResult validateResult) {
		connection.setResponse_body(null == connection.getResponse_body() ? new HashMap<>() : connection.getResponse_body());
		connection.getResponse_body().put("validate_details", validateResult.getValidateResultDetails());

		Map<String, Object> map = new HashMap<>();
		map.put("id", connection.getId());
		map.put("response_body", connection.getResponse_body());
		map.put("status", validateResult.getStatus());
		map.put("extParam", connection.getExtParam());

		return map;
	}

	private Map<String, Object> wrapConnectionResult(Connections connection, BaseConnectionValidateResult validateResult) {
		connection.getResponse_body().put("validate_details", validateResult.getValidateResultDetails());

		Map<String, Object> map = new HashMap<>();
		map.put("id", connection.getId());
		map.put("response_body", connection.getResponse_body());
		map.put("status", validateResult.getStatus());

		return map;
	}

	private Update getValidateResultUpdate(int retry, Long nextRetry, String status, Integer db_version, List<RelateDataBaseTable> schemaTables, List validateResultDetails, String dbFullVersion) {
		Update update = new Update();

		update.set("response_body.retry", retry);
		if (nextRetry != null) {
			update.set("response_body.next_retry", nextRetry);
		}

		update.set("response_body.validate_details", validateResultDetails);
		update.set("status", status);

		if (CollectionUtils.isNotEmpty(schemaTables)) {
			update.set("schema.tables", schemaTables);
		}

		if (db_version != null) {
			update.set("db_version", db_version);
		}

		if (db_version != null) {
			update.set("db_version", db_version);
		}

		if (dbFullVersion != null) {
			update.set("dbFullVersion", dbFullVersion);
		}

		return update;
	}
}
