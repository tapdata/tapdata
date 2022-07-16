package io.tapdata.websocket.handler;

import com.google.common.collect.ImmutableMap;
import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.validator.ConnectionValidateResult;
import com.tapdata.validator.ConnectionValidateResultDetail;
import com.tapdata.validator.ConnectionValidator;
import com.tapdata.validator.ValidatorConstant;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.TapInterface;
import io.tapdata.common.SettingService;
import io.tapdata.common.TapInterfaceUtil;
import io.tapdata.common.logging.error.ErrorCodeEnum;
import io.tapdata.common.logging.format.CustomerLogMessagesEnum;
import io.tapdata.entity.BaseConnectionValidateResult;
import io.tapdata.entity.BaseConnectionValidateResultDetail;
import io.tapdata.entity.ConnectionsType;
import io.tapdata.exception.ConnectionException;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.logging.JobCustomerLogger;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2020-12-22 15:39
 **/
@EventHandlerAnnotation(type = "testConnection")
public class TestConnectionHandler implements WebSocketEventHandler {

	private final static Logger logger = LogManager.getLogger(TestConnectionHandler.class);

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
					connection = clientMongoOperator.findOne(connectionIdQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
					save = true;
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
					clientMongoOperator.update(updateQuery, update, ConnectorConstant.CONNECTION_COLLECTION);
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
		clientMongoOperator.update(connectionIdQuery, update, ConnectorConstant.CONNECTION_COLLECTION);
		// Update connection options
		update = new Update().set("options", validateResult.getConnectionOptions());
		clientMongoOperator.update(connectionIdQuery, update, ConnectorConstant.CONNECTION_COLLECTION + "/connectionOptions");
	}

	@Deprecated
	private void updateConnectionUniqueName(Connections connection, Query connectionIdQuery) {
		connection.setUniqueName(connection.getId());
		clientMongoOperator.update(connectionIdQuery, new Update().set("uniqueName", connection.getUniqueName()), ConnectorConstant.CONNECTION_COLLECTION);
	}

	private void updateLoadSchemaFieldStatus(Connections connection, String status, Schema schema, String schemaVersion) {
		Query query = new Query(Criteria.where("_id").is(connection.getId()));
		Update update = new Update().set(ConnectorConstant.LOAD_FIELDS, status);
		if (null != schema && schema.isIncludeFields()) {
			update.set("schemaVersion", schemaVersion);
		}
		clientMongoOperator.update(query, update, ConnectorConstant.CONNECTION_COLLECTION);
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
				clientMongoOperator.update(connectionIdQuery, update, ConnectorConstant.CONNECTION_COLLECTION);
			}
		} else {
			clientMongoOperator.update(connectionIdQuery, update, ConnectorConstant.CONNECTION_COLLECTION);
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

	public static class TestConnectionResult {
		boolean host;
		boolean login;

		TestConnectionResult(boolean host, boolean login) {
			this.host = host;
			this.login = login;
		}
	}

	public static void testConnectionWithRetry(JobCustomerLogger customerLogger, Connections connections, String type) {
		if (!JobCustomerLogger.getFlag()) {
			return;
		}
		int retry = 0;
		long start = System.currentTimeMillis();
		TestConnectionResult result = null;
		while (retry < 3) {
			result = testConnection(customerLogger, connections, type);
			if (result.host && result.login) {
				break;
			}
			retry += 1;
		}
		if (!(result.host && result.login)) {
			ImmutableMap<String, Object> params = ImmutableMap.of(
					"dataSourceInfo", connections.getDataSourceInfo(),
					"takeMilliseconds", System.currentTimeMillis() - start,
					"type", type
			);
			if (!result.host) {
				customerLogger.error(
						CustomerLogMessagesEnum.AGENT_CHECK_CONNECTIVITY_STOP_RETRY,
						ErrorCodeEnum.DATASOURCE_AVAILABILITY_UNREACHABLE,
						params
				);
			} else if (!result.login) {
				customerLogger.error(
						CustomerLogMessagesEnum.AGENT_CHECK_CONNECTIVITY_STOP_RETRY,
						ErrorCodeEnum.DATASOURCE_AVAILABILITY_LOGIN_FAILED,
						params
				);
			}

			throw new ConnectionException("failed to connect to database: " + connections);
		}
	}

	public static TestConnectionResult testConnection(JobCustomerLogger customerLogger, Connections connections, String type) {
		// TODO: skip the test if the connections does not support it;
		TestConnectionResult result = new TestConnectionResult(true, true);
		long connectivityCost = 0;
		ErrorCodeEnum connectivityErrorCode = null;
		long loginCost = 0;
		ErrorCodeEnum loginErrorCode = null;
		String loginErrorMessage = null;

		ConnectionValidateResult validateResult = ConnectionValidator.initialValidate(connections);
		validateResult = ConnectionValidator.validate(connections, validateResult);
		if (validateResult != null) {
			String connectivityCode = null;
			String loginCode = null;
			DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(connections.getDatabase_type());
			switch (databaseTypeEnum) {
				case ORACLE:
					connectivityCode = ValidatorConstant.VALIDATE_CODE_ORACLE_HOST_IP;
					loginCode = ValidatorConstant.VALIDATE_CODE_ORACLE_USERNAME_PASSWORD;
					loginErrorCode = ErrorCodeEnum.DATASOURCE_AVAILABILITY_LOGIN_FAILED_DATASOURCE;
					break;
				case MYSQL:
				case MARIADB:
				case DAMENG:
				case HANA:
				case KUNDB:
				case ADB_MYSQL:
				case MYSQL_PXC:
					connectivityCode = ValidatorConstant.VALIDATE_CODE_MYSQL_HOST_IP;
					loginCode = ValidatorConstant.VALIDATE_CODE_MYSQL_USERNAME_PASSWORD;
					break;
				case MONGODB:
					connectivityCode = ValidatorConstant.VALIDATE_CODE_MONGODB_HOST_IP;
					loginCode = ValidatorConstant.VALIDATE_CODE_MONGODB_USERNAME_PASSWORD;
					break;
				case MSSQL:
					connectivityCode = ValidatorConstant.VALIDATE_CODE_MSSQL_HOST_IP;
					loginCode = ValidatorConstant.VALIDATE_CODE_MSSQL_USERNAME_PASSWORD;
					break;
				case SYBASEASE:
					connectivityCode = ValidatorConstant.VALIDATE_CODE_SYBASE_ASE_HOST_IP;
					loginCode = ValidatorConstant.VALIDATE_CODE_SYBASE_ASE_USERNAME_PASSWORD;
					break;
				case REDIS:
					connectivityCode = ValidatorConstant.VALIDATE_CODE_REDIS_HOST_IP;
					break;
			}
			for (ConnectionValidateResultDetail detail : validateResult.getValidateResultDetails()) {
				if (detail.getStage_code().equals(connectivityCode)) {
					if (detail.getError_code() != null) {
						connectivityErrorCode = ErrorCodeEnum.DATASOURCE_AVAILABILITY_UNREACHABLE;
					}
					connectivityCost = detail.getCost();
				} else if (detail.getStage_code().equals(loginCode)) {
					if (detail.getError_code() != null) {
						loginErrorCode = ErrorCodeEnum.DATASOURCE_AVAILABILITY_LOGIN_FAILED_DATASOURCE;
						loginErrorMessage = detail.getFail_message();
					}
					loginCost = detail.getCost();
				}
			}

			ImmutableMap<String, Object> connectParams = ImmutableMap.of(
					"dataSourceInfo", connections.getDataSourceInfo(),
					"takeMilliseconds", connectivityCost,
					"type", type
			);
			ImmutableMap<String, Object> loginParams = ImmutableMap.of(
					"dataSourceInfo", connections.getDataSourceInfo(),
					"takeMilliseconds", loginCost,
					"type", type
			);
			if (connectivityErrorCode != null) {
				customerLogger.error(connectivityErrorCode, connectParams);
			} else {
				result.host = true;
				if (connectivityCost > 1000) {
					customerLogger.warn(CustomerLogMessagesEnum.AGENT_CHECK_CONNECTIVITY_SLOW, connectParams);
				} else {
					customerLogger.info(CustomerLogMessagesEnum.AGENT_CHECK_CONNECTIVITY_CONNECTED, connectParams);
				}
				if (loginErrorCode != null) {
					if (loginErrorCode.isDatasourceError()) {
						customerLogger.error(connections.getDatabase_type(), loginErrorMessage, loginErrorCode, loginParams);
					} else {
						customerLogger.error(loginErrorCode, loginParams);
					}
				} else {
					result.login = true;
					if (loginCost > 1000) {
						customerLogger.warn(CustomerLogMessagesEnum.AGENT_CHECK_LOGIN_SLOW, loginParams);
					} else {
						customerLogger.info(CustomerLogMessagesEnum.AGENT_CHECK_LOGIN_CONNECTED, loginParams);
					}
				}
			}
		} else {
			TapInterface tapInterface = TapInterfaceUtil.getTapInterface(connections.getDatabase_type(), null);
			if (tapInterface != null) {
				ImmutableMap<String, Object> connectParams = ImmutableMap.of(
						"dataSourceInfo", connections.getDataSourceInfo(),
						"takeMilliseconds", connectivityCost,
						"type", type
				);
				ImmutableMap<String, Object> loginParams = ImmutableMap.of(
						"dataSourceInfo", connections.getDataSourceInfo(),
						"takeMilliseconds", loginCost,
						"type", type
				);
				tapInterface.connectionsInit(ConnectionsType.getConnectionType(connections.getConnection_type()));
				BaseConnectionValidateResult baseConnectionValidateResult = tapInterface.testConnections(connections);
				for (BaseConnectionValidateResultDetail detail : baseConnectionValidateResult.getValidateResultDetails()) {
					if ("host".equals(detail.getCode()) || TestConnectionItemConstant.CHECK_CONNECT.equalsIgnoreCase(detail.getShow_msg())) {
						if (detail.getStatus().equals(BaseConnectionValidateResultDetail.VALIDATE_DETAIL_RESULT_FAIL)) {
							customerLogger.error(ErrorCodeEnum.DATASOURCE_AVAILABILITY_UNREACHABLE, connectParams);
							result.host = false;
						} else {
							if (detail.getCost() > 1000) {
								customerLogger.warn(CustomerLogMessagesEnum.AGENT_CHECK_CONNECTIVITY_SLOW, connectParams);
							} else {
								customerLogger.info(CustomerLogMessagesEnum.AGENT_CHECK_CONNECTIVITY_CONNECTED, connectParams);
							}
						}
					} else if ("login".equals(detail.getCode()) || TestConnectionItemConstant.CHECK_AUTH.equalsIgnoreCase(detail.getShow_msg())) {
						if (detail.getStatus().equals(BaseConnectionValidateResultDetail.VALIDATE_DETAIL_RESULT_FAIL)) {
							customerLogger.error(CustomerLogMessagesEnum.AGENT_CHECK_LOGIN_DISCONNECTED, ErrorCodeEnum.DATASOURCE_AVAILABILITY_LOGIN_FAILED_DATASOURCE, loginParams);
							result.host = false;
						} else if (detail.getStatus().equals(BaseConnectionValidateResultDetail.VALIDATE_DETAIL_RESULT_PASSED)) {
							if (detail.getCost() > 1000) {
								customerLogger.warn(CustomerLogMessagesEnum.AGENT_CHECK_LOGIN_SLOW, loginParams);
							} else {
								customerLogger.info(CustomerLogMessagesEnum.AGENT_CHECK_LOGIN_CONNECTED, loginParams);
							}
						}
					}
				}
			}
		}

		return result;
	}
}
