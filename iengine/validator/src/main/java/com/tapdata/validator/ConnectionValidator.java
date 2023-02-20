package com.tapdata.validator;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.TestConnectionItemConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Schema;
import io.tapdata.entity.BaseConnectionValidateResult;
import io.tapdata.entity.BaseConnectionValidateResultDetail;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectionValidator {
	public static final String TAG = ConnectionValidator.class.getSimpleName();
	protected static Logger logger = LogManager.getLogger(ConnectionValidator.class);

	public static ConnectionValidateResult initialValidate(Connections connections) {
		// initial validate result
		ConnectionValidateResult validateResult = new ConnectionValidateResult();
		List<ConnectionValidateResultDetail> validateResultDetails = new ArrayList<>();
		validateResult.setValidateResultDetails(validateResultDetails);

		// get retry and next_try info
		Map<String, Object> responseBody = connections.getResponse_body();
		if (MapUtils.isNotEmpty(responseBody) && responseBody.containsKey("retry")) {
			Object retry = responseBody.get("retry");
			validateResult.setRetry(retry == null ? 0 : (Integer) retry);
		}

		String databaseType = connections.getDatabase_type();
		String connectionType = connections.getConnection_type();

		List<ValidateItemEnum> validateItemEnums = ValidateItemEnum.fromConnectionType(databaseType);
		if (CollectionUtils.isNotEmpty(validateItemEnums)) {
			for (ValidateItemEnum validateItem : validateItemEnums) {
				String validateItemConnectionType = validateItem.getConnectionType();
				if (validateItemConnectionType.equals(connectionType) ||
						ConnectorConstant.CONNECTION_TYPE_SOURCE_TARGET.equals(validateItemConnectionType) ||
						ConnectorConstant.CONNECTION_TYPE_SOURCE_TARGET.equals(connectionType)) {
					String typeCode = validateItem.getStage_code();
					String showMsg = validateItem.getShow_msg();
					int sort = validateItem.getSort();
					boolean required = validateItem.getRequired();
					ConnectionValidateResultDetail validateResultDetail = new ConnectionValidateResultDetail(typeCode, showMsg, sort, required);
					validateResultDetails.add(validateResultDetail);
				}
			}
		}
		logger.debug("validateResult is: {}", validateResult);
		return validateResult;
	}

	public static ConnectionValidateResult validate(Connections connections, ConnectionValidateResult previousValidateResult) {

		return null;
	}

	public static boolean continueValidateConnection(ConnectionValidateResult previousValidateResult, ConnectionValidateResultDetail validateResultDetail) {
		String status = validateResultDetail.getStatus();
		boolean required = validateResultDetail.getRequired();
		if (!ValidatorConstant.VALIDATE_DETAIL_RESULT_PASSED.equalsIgnoreCase(status) && required) {
			int retry = previousValidateResult.getRetry();
			retry++;
			int interval = 10; // unit second
			Calendar calendar = Calendar.getInstance(); // gets a calendar using the default time zone and locale.
			calendar.add(Calendar.SECOND, interval * retry);
			long nextRetry = calendar.getTimeInMillis();

			previousValidateResult.setRetry(retry);
			previousValidateResult.setNextRetry(nextRetry);

			previousValidateResult.setStatus(ValidatorConstant.CONNECTION_STATUS_INVALID);
			return true;
		}
		return false;
	}

	public static boolean continueValidateConnection(BaseConnectionValidateResult previousValidateResult, BaseConnectionValidateResultDetail validateResultDetail) {
		String status = validateResultDetail.getStatus();
		boolean required = validateResultDetail.isRequired();
		if (!ValidatorConstant.VALIDATE_DETAIL_RESULT_PASSED.equalsIgnoreCase(status) && required) {
			int retry = previousValidateResult.getRetry();
			retry++;
			int interval = 10; // unit second
			Calendar calendar = Calendar.getInstance(); // gets a calendar using the default time zone and locale.
			calendar.add(Calendar.SECOND, interval * retry);
			long nextRetry = calendar.getTimeInMillis();

			previousValidateResult.setRetry(retry);
			previousValidateResult.setNextRetry(nextRetry);

			previousValidateResult.setStatus(ValidatorConstant.CONNECTION_STATUS_INVALID);
			return true;
		}
		return false;
	}

	public static boolean validateHostPort(String databaseHost, Integer databasePort) {
		try {
			Socket s = new Socket();
			s.connect(new InetSocketAddress(databaseHost, databasePort), 10000);
			s.close();
			return true;
		} catch (Exception e) {
			logger.error("Check ip {} port {} failed {}", databaseHost, databasePort, e.getMessage(), e);
			return false;
		}
	}

	public static void setSQLExceptionResultDetail(ConnectionValidateResultDetail validateResultDetail, SQLException se, String validateErrorCode) {
		int errorCode = se.getErrorCode();
		String message = se.getMessage();
		StringBuilder sb = new StringBuilder(errorCode + "").append(": ").append(message);
		validateResultDetail.setFail_message(sb.toString());
		validateResultDetail.setError_code(validateErrorCode);
		validateResultDetail.setStatus(ValidatorConstant.VALIDATE_DETAIL_RESULT_FAIL);
	}

	public static void releaseConnectionResource(Connection connection, Statement statement, ResultSet resultSet) {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			if (statement != null) {
				statement.close();
			}
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			// ignore exce[tion
		}
	}

	public static void sortValidateDetails(List<ConnectionValidateResultDetail> initialValidateResult) {
		Collections.sort(initialValidateResult, (detail1, detail2) -> {

			int sort1 = detail1.getSort();
			int sort2 = detail2.getSort();

			return Integer.compare(sort1, sort2);
		});
	}

	public static void setResult(String failMessage, ConnectionValidateResultDetail validateResultDetail) {
		String errorCode = ValidatorConstant.ERROR_CODE_GRIDFS_DIR_EXIST;
		if (StringUtils.isNotBlank(failMessage)) {
			validateResultDetail.setError_code(errorCode);
			validateResultDetail.setFail_message(failMessage);
			validateResultDetail.setStatus(ValidatorConstant.VALIDATE_DETAIL_RESULT_FAIL);
		} else {
			validateResultDetail.setStatus(ValidatorConstant.VALIDATE_DETAIL_RESULT_PASSED);
		}
	}

	/**
	 * Do pdk connection test
	 *
	 * @param connections  Connection Data
	 * @param databaseType Pdk spec json
	 * @return {@link ConnectionValidateResult}
	 */
	public static ConnectionValidateResult testPdkConnection(Connections connections, DatabaseTypeEnum.DatabaseType databaseType) {

		ConnectionValidateResult connectionValidateResult = new ConnectionValidateResult();
		if ("pdk".equals(connections.getPdkType())) {
			ConnectionNode connectionNode = null;
			long ts = System.currentTimeMillis();
			try {
				// Create connection node
				connectionNode = PDKIntegration.createConnectionConnectorBuilder()
						.withConnectionConfig(DataMap.create(connections.getConfig()))
						.withGroup(databaseType.getGroup())
						.withPdkId(databaseType.getPdkId())
						.withAssociateId(connections.getName() + "_" + ts)
						.withVersion(databaseType.getVersion())
						.withLog(new TapLog())
						.build();
				ConnectionNode finalConnectionNode = connectionNode;
				List<ConnectionValidateResultDetail> resultDetails = new ArrayList<>();
				AtomicBoolean anyErrorOccurred = new AtomicBoolean(false);
				AtomicReference<ConnectionOptions> connectionOptionsAtomicReference = new AtomicReference<>();
				// Call pdk connectionTest function
				PDKInvocationMonitor.invoke(connectionNode, PDKMethod.CONNECTION_TEST,
						() -> connectionOptionsAtomicReference.set(finalConnectionNode.connectionTest(testItem -> {
							// Handle test item result
							final String item = testItem.getItem();
							final int result = testItem.getResult();

							ConnectionValidateResultDetail resultDetail = new ConnectionValidateResultDetail();
							resultDetail.setStatus(
									result != TestItem.RESULT_SUCCESSFULLY ?
											ValidatorConstant.VALIDATE_DETAIL_RESULT_FAIL :
											ValidatorConstant.VALIDATE_DETAIL_RESULT_PASSED
							);
							if (result == TestItem.RESULT_FAILED) {
								anyErrorOccurred.set(true);
							}
							resultDetail.setRequired(result != TestItem.RESULT_SUCCESSFULLY_WITH_WARN);
							resultDetail.setFail_message(testItem.getInformation());
							resultDetail.setShow_msg(item);
							resultDetails.add(resultDetail);
						})), TAG, TAG, error -> {
							ConnectionValidateResultDetail resultDetail = new ConnectionValidateResultDetail();
							resultDetail.setStatus(ValidatorConstant.VALIDATE_DETAIL_RESULT_FAIL);
							resultDetail.setRequired(true);
							resultDetail.setFail_message(error.getMessage());
							resultDetail.setShow_msg("Occurred error");
							resultDetails.add(resultDetail);
							anyErrorOccurred.set(true);
						});
				if (null == connectionOptionsAtomicReference.get()) {
					connectionOptionsAtomicReference.set(ConnectionOptions.create());
				}
				connectionValidateResult.setConnectionOptions(connectionOptionsAtomicReference.get());

				// Call pdk tableCount function to get the number of resources(table,api,file...)
				if (CollectionUtils.isNotEmpty(resultDetails)) {
					connectionValidateResult.setValidateResultDetails(resultDetails);
					connectionValidateResult.setStatus(
							anyErrorOccurred.get() ?
									ValidatorConstant.CONNECTION_STATUS_INVALID :
									ValidatorConstant.CONNECTION_STATUS_READY
					);
					AtomicInteger schemaCount = new AtomicInteger();
					if (ValidatorConstant.CONNECTION_STATUS_READY.equals(connectionValidateResult.getStatus())) {
						PDKInvocationMonitor.invoke(connectionNode, PDKMethod.INIT, connectionNode::connectorInit, "Init PDK", TAG);
						try {
							PDKInvocationMonitor.invoke(
									finalConnectionNode,
									PDKMethod.TABLE_COUNT,
									() -> schemaCount.set(finalConnectionNode.tableCount()),
									"Table count",
									TAG
							);
							if (schemaCount.get() <= 0) {
								ConnectionValidateResultDetail notFoundAnySchema = new ConnectionValidateResultDetail(TestConnectionItemConstant.LOAD_SCHEMA, "Load Schema", 99, false);
								notFoundAnySchema.failed("Not found any schema");
								resultDetails.add(notFoundAnySchema);
							}
						} catch (Exception e) {
							ConnectionValidateResultDetail tableCountFailed = new ConnectionValidateResultDetail(TestConnectionItemConstant.LOAD_SCHEMA, "Load Schema", 99, false);
							tableCountFailed.failed(String.format("Get table count failed %s", e.getMessage()));
							resultDetails.add(tableCountFailed);
							logger.error("Get table count failed {}", e.getMessage(), e);
						} finally {
							PDKInvocationMonitor.invoke(connectionNode, PDKMethod.STOP, connectionNode::connectorStop, "Stop PDK", TAG);
						}
					}
					connectionValidateResult.setSchema(new Schema(false, schemaCount.get()));
				}
			} finally {
				if(connectionNode != null)
					connectionNode.unregisterMemoryFetcher();
				PDKIntegration.releaseAssociateId(connections.getName() + "_" + ts);
			}
		}
		return connectionValidateResult;
	}
}
