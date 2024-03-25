package io.tapdata.websocket.handler;

import com.google.common.collect.Maps;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Schema;
import com.tapdata.entity.TapLog;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.validator.ConnectionValidateResult;
import com.tapdata.validator.ConnectionValidateResultDetail;
import com.tapdata.validator.ConnectionValidator;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.aspect.supervisor.AspectRunnableUtil;
import io.tapdata.aspect.supervisor.DisposableThreadGroupAspect;
import io.tapdata.aspect.supervisor.entity.ConnectionTestEntity;
import io.tapdata.common.SettingService;
import io.tapdata.entity.BaseConnectionValidateResult;
import io.tapdata.exception.ConnectionException;
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.utils.DisposableType;
import io.tapdata.utils.ConnectionUpdateOperation;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import io.tapdata.websocket.testconnection.RocksDBTestConnectionImpl;
import io.tapdata.websocket.testconnection.TestConnection;
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

	private static final Logger logger = LogManager.getLogger(TestConnectionHandler.class);

	static Logger getLogger() {
		return logger;
	}

	public static final String COLLECTION_CONNECTION_OPTIONS = ConnectorConstant.CONNECTION_COLLECTION + "/connectionOptions";
	private static final String NOT_CHANGE_LAST_COLLECTION = ConnectorConstant.CONNECTION_COLLECTION;
	protected String collection = NOT_CHANGE_LAST_COLLECTION;

	private ClientMongoOperator clientMongoOperator;
	private SettingService settingService;
	protected Query connectionIdQuery; // if need to save, query is not null

	/**
	 * 初始化handler方法
	 *
	 * @param clientMongoOperator 查询管理端数据
	 */
	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
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
	 * @param eventData
	 * @param sendMessage
	 * @return
	 */
	@Override
	public Object handle(Map eventData, SendMessage sendMessage) {
		Map<String, Object> event = (Map<String, Object>) eventData;
		getLogger().info("Test connection, event: {}", event);
		if (MapUtils.isEmpty(event)) {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_CONNECTION_RESULT, "Event data cannot be empty");
		}

		Connections connection = parseConnection(event);
		String connectionId = ConnectionUpdateOperation.ID.getString(event);
		String connName = ConnectionUpdateOperation.NAME.getOrDefault(event, "");
		String pdkHash = ConnectionUpdateOperation.PDK_HASH.getOrDefault(event, "");

		ConnectionTestEntity entity = new ConnectionTestEntity()
			.associateId(UUID.randomUUID().toString())
			.time(System.nanoTime())
			.connectionId(connectionId)
			.type(ConnectionUpdateOperation.TYPE.getString(event))
			.connectionName(connName)
			.pdkType(ConnectionUpdateOperation.PDK_TYPE.getString(event))
			.pdkHash(pdkHash)
			.schemaVersion(ConnectionUpdateOperation.SCHEMA_VERSION.getString(event))
			.databaseType(ConnectionUpdateOperation.DATABASE_TYPE.getString(event));
		String threadName = String.format("TEST-CONNECTION-%s", connName);
		DisposableThreadGroup threadGroup = new DisposableThreadGroup(DisposableType.CONNECTION_TEST, threadName);

		AspectRunnableUtil.aspectAndStart(new DisposableThreadGroupAspect<>(connectionId, threadGroup, entity)
			, () -> handleSync(event, sendMessage, connection)
		);
		return null;
	}


	protected void handleSync(Map<String, Object> event, SendMessage<WebSocketEventResult> sendMessage, Connections connection) {
		try {
			getLogger().info(TapLog.CON_LOG_0021.getMsg(), connection.getName());

			DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(connection.getDatabase_type());
			boolean editTest = ConnectionUpdateOperation.EDIT_TEST.getIfExist(event, false);
			boolean isExternalStorage = ConnectionUpdateOperation.IS_EXTERNAL_STORAGE_ID.getIfExist(event, false);
			boolean updateSchema = isUpdateSchema(databaseTypeEnum, event, true);
			boolean everLoadSchema = everLoadSchema(databaseTypeEnum, event, false);

			setQueryAndExtParam(event, connection, editTest, isExternalStorage);

			// Decode passwords
			connection.decodeDatabasePassword();
			resetPasswordOfPlainPassword(connection, event);

			// PDK connection test
			if (StringUtils.isBlank(connection.getPdkType()) && isExternalStorage) {
				handleNoPdkTestConnection(sendMessage, event);
				return;
			}

			DatabaseTypeEnum.DatabaseType databaseDefinition = getDatabaseType(connection);
			testPdkConnection(sendMessage, connection, databaseDefinition, connectionIdQuery, updateSchema, everLoadSchema, isExternalStorage);
		} catch (Exception e) {
			String connName = ConnectionUpdateOperation.NAME.getOrDefault(event, "");
			handleError(event, connection, connName, sendMessage, e);
		}
	}

	protected Connections parseConnection(Map<String, Object> event) {
		Connections connection;
		try {
			Object schema = ConnectionUpdateOperation.SCHEMA.get(event);
			if (schema instanceof Map) {
				event.remove(ConnectionUpdateOperation.SCHEMA.getFullKey());
			}
			connection = JSONUtil.map2POJO(event, Connections.class);
		} catch (Exception e) {
			throw new ConnectionException("Map convert to Connections failed: " + e.getMessage(), e);
		}

		String pdkHash = ConnectionUpdateOperation.PDK_HASH.getOrDefault(event, "");
		connection.setPdkHash(pdkHash);
		return connection;
	}

	protected boolean isUpdateSchema(DatabaseTypeEnum databaseTypeEnum, Map<String, Object> event, boolean def) {
		switch (databaseTypeEnum) {
			case FILE:
				return false;
			default:
				return ConnectionUpdateOperation.UPDATE_SCHEMA.getIfExist(event, def);
		}
	}

	protected boolean everLoadSchema(DatabaseTypeEnum databaseTypeEnum, Map<String, Object> event, boolean def) {
		switch (databaseTypeEnum) {
			case FILE:
				return true;
			default:
				return ConnectionUpdateOperation.EVER_LOAD_SCHEMA.getIfExist(event, def);
		}
	}

	protected Connections setQueryAndExtParam(Map<String, Object> event, Connections connection, boolean editTest, boolean isExternalStorage) {
		if (isExternalStorage) {
			collection = ConnectorConstant.EXTERNAL_STORAGE_COLLECTION + "/set";
			String connName = ConnectionUpdateOperation.NAME.getOrDefault(event, "");
			connection.setName(connName);
		}

		String id = ConnectionUpdateOperation.EXTERNAL_STORAGE_ID.getIfExist(event, connection.getId());
		if (StringUtils.isBlank(id) || editTest) return connection;

		connectionIdQuery = new Query(Criteria.where("_id").is(id));
		connectionIdQuery.fields().exclude("schema");

		if (isExternalStorage) return connection;

		Connections newConnection = clientMongoOperator.findOne(connectionIdQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
		if (newConnection != null) {
			connection = newConnection;
			Map extParam = Maps.newHashMap();
			extParam = ConnectionUpdateOperation.EXT_PARAM.getOrDefault(event, extParam);
			connection.setExtParam(extParam);
		}

		return connection;
	}

	protected void resetPasswordOfPlainPassword(Connections connection, Map<String, Object> event) {
		if (StringUtils.isBlank(connection.getDatabase_password()) && event.containsKey(ConnectionUpdateOperation.PLAIN_PASSWORD.getFullKey())) {
			String plainPassword = ConnectionUpdateOperation.PLAIN_PASSWORD.getString(event);
			connection.setDatabase_password(plainPassword);
		}
	}

	protected DatabaseTypeEnum.DatabaseType getDatabaseType(Connections connection) {
		if (StringUtils.isBlank(connection.getPdkType())) {
			throw new ConnectionException("Unknown connection pdk type");
		}

		DatabaseTypeEnum.DatabaseType databaseDefinition = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());
		if (databaseDefinition == null) {
			throw new ConnectionException(String.format("Unknown database type %s", connection.getDatabase_type()));
		}
		downloadPdkFile(databaseDefinition);
		return databaseDefinition;
	}

	protected void downloadPdkFile(DatabaseTypeEnum.DatabaseType databaseDefinition) {
		PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator, databaseDefinition.getPdkHash(), databaseDefinition.getJarFile(), databaseDefinition.getJarRid());
	}

	protected void testPdkConnection(SendMessage<WebSocketEventResult> sendMessage, Connections connection, DatabaseTypeEnum.DatabaseType databaseDefinition,
																	 Query connectionIdQuery, boolean updateSchema, boolean everLoadSchema, boolean isExternalStorage) throws IOException {
		ConnectionValidateResult validateResult = ConnectionValidator.testPdkConnection(connection, databaseDefinition);
		Schema schema = validateResult.getSchema();
		List<ConnectionValidateResultDetail> validateResultDetails = validateResult.getValidateResultDetails();
		// Websocket returns test item(s)
		sendMessage.send(WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.TEST_CONNECTION_RESULT, wrapConnectionResult(connection, validateResult)));
		if (null != connectionIdQuery) {
			// Update connection test result
			updatePDKConnectionTestResult(connectionIdQuery, updateSchema, everLoadSchema, validateResult, validateResultDetails, isExternalStorage, connection.getName());
		}

		if (needLoadField(updateSchema, schema, everLoadSchema) && null != connectionIdQuery) {
			// Load schema field
			LoadSchemaRunner loadSchemaRunner = new LoadSchemaRunner(connection, clientMongoOperator,
				null == schema.getTables() ? schema.getTapTableCount() : schema.getTables().size());
			loadSchemaRunner.run();
		} else {
			updateLoadSchemaFieldStatus(isExternalStorage, connection, ConnectorConstant.LOAD_FIELD_STATUS_FINISHED, schema, UUIDGenerator.uuid());
		}
	}

	protected void handleError(Map<String, Object> event, Connections connection, String connName, SendMessage<WebSocketEventResult> sendMessage, Exception e) {
		Optional.of(event).ifPresent(map -> map.remove("config"));
		String errMsg = null;
		try {
			if (TmUnavailableException.isInstance(e)) {
				connection = null;
				errMsg = String.format("Test connection '%s' failed because TM unavailable, eventId: '%s', err: %s", connName, event.get("id"), e.getMessage());
				getLogger().warn(errMsg);
				sendMessage.send(WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_CONNECTION_RESULT, errMsg));
			} else {
				errMsg = String.format("Test connection %s failed, data: %s, err: %s", connName, event, e.getMessage());
				getLogger().error(errMsg, e);
				sendMessage.send(WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_CONNECTION_RESULT, errMsg, e));
			}
		} catch (Exception ex) {
			getLogger().error(String.format("Send error test connection result to websocket failed, msg: %s, root exception: %s", ex.getMessage(), errMsg), ex);
		}

		updateConnectionInvalid(connection);
	}

	protected void updateConnectionInvalid(Connections connection) {
		if (null == connection) return;

		Update update = new Update();
		ConnectionUpdateOperation.ResponseBody.RETRY.set(update, 0);
		ConnectionUpdateOperation.ResponseBody.NEXT_RETRY.setIfNotNull(update, 0L);
		ConnectionUpdateOperation.STATUS.set(update, BaseConnectionValidateResult.CONNECTION_STATUS_INVALID);

		Query updateQuery = new Query(where("_id").is(connection.getId()));
		clientMongoOperator.update(updateQuery, update, collection);
	}

	protected void updatePDKConnectionTestResult(Query connectionIdQuery, boolean updateSchema, boolean everLoadSchema
		, ConnectionValidateResult validateResult, List<ConnectionValidateResultDetail> validateResultDetails
		, boolean isExternalStorage, String name) {
		Update update = new Update();
		// Update connection
		ConnectionUpdateOperation.ResponseBody.RETRY.set(update, validateResult.getRetry());
		ConnectionUpdateOperation.ResponseBody.NEXT_RETRY.setIfNotNull(update, validateResult.getNextRetry());
		ConnectionUpdateOperation.ResponseBody.VALIDATE_DETAILS.set(update, validateResultDetails);
		ConnectionUpdateOperation.STATUS.set(update, validateResult.getStatus());
		boolean needLoadField = needLoadField(updateSchema, validateResult.getSchema(), everLoadSchema);
		ConnectionUpdateOperation.LOAD_SCHEMA_FIELD.set(update, needLoadField);
		ConnectionUpdateOperation.NAME.set(update, isExternalStorage, name);

		clientMongoOperator.update(connectionIdQuery, update, collection);
		if (!isExternalStorage) {
			// Update connection options
			update = new Update();
			ConnectionUpdateOperation.CONNECTION_OPTIONS.set(update, validateResult.getConnectionOptions());
			clientMongoOperator.update(connectionIdQuery, update, COLLECTION_CONNECTION_OPTIONS);
		}
	}

	protected void updateLoadSchemaFieldStatus(boolean isExternalStorage, Connections connection, String status, Schema schema, String schemaVersion) {
		if (isExternalStorage) return;

		Query query = new Query(Criteria.where("_id").is(connection.getId()));
		Update update = new Update();
		ConnectionUpdateOperation.LOAD_FIELDS_STATUS.set(update, status);
		ConnectionUpdateOperation.SCHEMA_VERSION.set(update, null != schema && schema.isIncludeFields(), schemaVersion);
		clientMongoOperator.update(query, update, collection);
	}

	protected boolean needLoadField(boolean updateSchema, Schema schema, boolean everLoadSchema) {
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

	private Map<String, Object> wrapConnectionResult(Connections connection, ConnectionValidateResult validateResult) {
		connection.setResponse_body(null == connection.getResponse_body() ? new HashMap<>() : connection.getResponse_body());
		connection.getResponse_body().put(ConnectionUpdateOperation.ResponseBody.VALIDATE_DETAILS.getKey(), validateResult.getValidateResultDetails());

		Map<String, Object> map = new HashMap<>();
		ConnectionUpdateOperation.ID.set(map, connection.getId());
		ConnectionUpdateOperation.RESPONSE_BODY.set(map, connection.getResponse_body());
		ConnectionUpdateOperation.STATUS.set(map, validateResult.getStatus());
		ConnectionUpdateOperation.EXT_PARAM.set(map, connection.getExtParam());

		return map;
	}

	protected void handleNoPdkTestConnection(SendMessage sendMessage, Map event) throws IOException {
		ConnectionValidateResult connectionValidateResult = new ConnectionValidateResult();
		TestConnection testConnection = getInstance(event.get("testType").toString());
		testConnection.testConnection(event, connectionValidateResult);
		sendMessage.send(WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.TEST_CONNECTION_RESULT,
			wrapConnectionResult(new Connections(), connectionValidateResult)));
	}

	protected TestConnection getInstance(String testType) {
		switch (testType) {
			case "rocksdb":
				return new RocksDBTestConnectionImpl();
			default:
				throw new ConnectionException("TestType not found instance '" + testType + "'");
		}
	}

}
