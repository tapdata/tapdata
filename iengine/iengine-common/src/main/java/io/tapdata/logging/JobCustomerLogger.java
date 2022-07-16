package io.tapdata.logging;

import com.google.common.collect.ImmutableMap;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.AppType;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.logging.error.ErrorCodeEnum;
import io.tapdata.common.logging.format.CustomerLogMessagesEnum;
import org.bson.Document;

import java.util.Collections;


/**
 * @author Dexter
 */
public class JobCustomerLogger {
  public static String LOG_LEVEL_INFO = "INFO";
  public static String LOG_LEVEL_WARN = "WARN";
  public static String LOG_LEVEL_ERROR = "ERROR";
  public static String LOG_LEVEL_FATAL = "FATAL";

  public static final String CUSTOMER_ERROR_LOG_PREFIX = "#ERROR#";

  /**
   * flag of customer log, only DAAS opens customer log for now.
   */
  private final static boolean customerLogFlag = AppType.init().isDaas();

  private String dataFlowId;
  private String jobName;
  private ClientMongoOperator clientMongoOperator;

  public static boolean getFlag() {
    return customerLogFlag;
  }

  private static Document newLogDocument(String key, String level, String dataFlowId) {
    Document doc = new Document();
    doc.put("version", 1);

    doc.put("key", key);
    doc.put("level", level);
    doc.put("dataFlowId", dataFlowId);
    long ts = System.currentTimeMillis();
    doc.put("timestamp", ts);
    doc.put("date", new java.util.Date(ts));

    return doc;
  }

  public static void unknownError(String dataFlowId, String jobName, ClientMongoOperator clientMongoOperator, String error) {
    if (!customerLogFlag) {
      return;
    }

    Document doc = newLogDocument(CustomerLogMessagesEnum.AGENT_ERROR.getKey(), LOG_LEVEL_ERROR, dataFlowId);
    Document params = new Document();
    params.put("jobName", jobName);
    params.put("errorCode", ErrorCodeEnum.UNKNOWN.getCode());
    params.put("error", error);
    doc.put("jobName", jobName);
    doc.put("params", params);
    doc.put("templateKeys", Collections.singletonList("errorMessage"));

    clientMongoOperator.insertOne(doc, ConnectorConstant.CUSTOMER_LOG_COLLECTION);
  }

  public static void dataflowStopped(String dataFlowId, String dataFlowName, ClientMongoOperator clientMongoOperator) {
    if (!customerLogFlag) {
      return;
    }

    String key = CustomerLogMessagesEnum.AGENT_DATAFLOW_STOPPED.getKey();
    Document doc = newLogDocument(key, LOG_LEVEL_INFO, dataFlowId);
    Document params = new Document();
    params.put("dataFlowName", dataFlowName);
    doc.put("params", params);

    clientMongoOperator.insertOne(doc, ConnectorConstant.CUSTOMER_LOG_COLLECTION);
  }

  /**
   * This empty customer logger is provided to avoid NPE
   */
  public JobCustomerLogger() {}

  public JobCustomerLogger(String dataFlowId, String jobName, ClientMongoOperator clientMongoOperator) {
    this.jobName = jobName;
    this.dataFlowId = dataFlowId;
    this.clientMongoOperator = clientMongoOperator;
  }

  public boolean isEmpty() {
    return dataFlowId == null && jobName == null && clientMongoOperator == null;
  }

  private Document newInfoLogDocument(String key) {
    return newLogDocument(key, LOG_LEVEL_INFO, dataFlowId);
  }

  private Document newWarnLogDocument(String key) {
    return newLogDocument(key, LOG_LEVEL_WARN, dataFlowId);
  }

  private Document newErrorFatalLogDocument(String key, ErrorCodeEnum errorCode) {
    if (errorCode.getCode() >= 90000) {
      return newLogDocument(key, LOG_LEVEL_FATAL, dataFlowId);
    }
    return newLogDocument(key, LOG_LEVEL_ERROR, dataFlowId);
  }

  private Document newJobLogParamsDocument() {
    Document params = new Document();
    params.put("jobName", jobName);

    return params;
  }

  private Document newJobLogParamsDocument(ImmutableMap<String, Object> params) {
    Document paramsDoc = newJobLogParamsDocument();
    if (params == null) {
      return paramsDoc;
    }

    for (String key : params.keySet()) {
      paramsDoc.put(key, params.get(key).toString());
    }

    return paramsDoc;
  }


  private void saveToDb(Document document) {
    clientMongoOperator.insertOne(document, ConnectorConstant.CUSTOMER_LOG_COLLECTION);
  }

  /**
   * This function adds an info level customer log.
   *
   * <p> Params should be provided if the customer log has args in it. </p>
   */
  public void info(CustomerLogMessagesEnum key, ImmutableMap<String, Object> params) {
    if (!customerLogFlag || isEmpty()) {
      return;
    }

    Document entity  = newInfoLogDocument(key.getKey());
    // params inner document
    entity.put("params", newJobLogParamsDocument(params));

    saveToDb(entity);
  }

  /**
   * This function adds an info level customer log.
   *
   * <p> This is a shortcut for adding an info level customer without params, please
   * make sure that the customer log does not have params. </p>
   */
  public void info(CustomerLogMessagesEnum key) {
    info(key, null);
  }

  /**
   * This function adds a warning level customer log.
   *
   * <p> Params should be provided if the customer log has args in it. </p>
   */
  public void warn(CustomerLogMessagesEnum key, ImmutableMap<String, Object> params) {
    if (!customerLogFlag || isEmpty()) {
      return;
    }

    Document entity  = newWarnLogDocument(key.getKey());
    // params inner document
    entity.put("params", newJobLogParamsDocument(params));

    saveToDb(entity);
  }

  /**
   * This function adds a warning level customer log
   *
   * <p> This is a shortcut for adding a warning level customer without params, please
   * make sure that the customer log does not have params. </p>
   */
  public void warn(CustomerLogMessagesEnum key) {
    info(key, null);
  }


  /**
   * This function adds an agent error level customer log.
   *
   * <p> In some cases, the agent error level customer logs may have its' own log message
   * format instead of using {@code CustomerLogMessagesEnum.AGENT_ERROR}, this function is
   * provided for situations like these. </p>
   *
   * <p> The error codes should always be provided so that it can be shown in customer log,
   * all error codes should be defined in {@link ErrorCodeEnum}. </p>
   *
   * <p> Params should be provided if the customer log has args in it. </p>
   */
  public void error(CustomerLogMessagesEnum key, ErrorCodeEnum errorCode, ImmutableMap<String, Object> params) {
    if (!customerLogFlag || isEmpty()) {
      return;
    }

    Document entity  = newErrorFatalLogDocument(key.getKey(), errorCode);

    // params inner document
    Document paramsDoc = newJobLogParamsDocument(params);
    paramsDoc.put("errorCode", errorCode.getCode());
    entity.put("params", paramsDoc);
    if (params != null) {
      entity.put("templateKeys", Collections.singletonList("errorMessage"));
    }

    saveToDb(entity);
  }

  /**
   * This function adds an agent error level customer log.
   *
   * <p> Same as {@code error(CustomerLogMessagesEnum key, ErrorCodeEnum errorCode,
   * ImmutableMap<String, Object> params)}, the difference is that in this function params
   * are not needed since some agent error level customer logs do not have args. </p>
   */
  public void error(CustomerLogMessagesEnum key, ErrorCodeEnum errorCode) {
    error(key, errorCode, null);
  }

  /**
   * This function adds an agent error level customer log.
   *
   * <p> In most of the cases, the agent error level customer logs always using
   * {@code CustomerLogMessagesEnum.AGENT_ERROR}, so the key can be ignored. </p>
   */
  public void error(ErrorCodeEnum errorCode, ImmutableMap<String, Object> params) {
    error(CustomerLogMessagesEnum.AGENT_ERROR, errorCode, params);
  }

  /**
   * This function adds an agent error level customer log.
   *
   * <p> Same as {@code error(ErrorCodeEnum errorCode, ImmutableMap<String, Object> params)},
   * the difference is that in this function params are not needed since some agent error
   * level customer logs do not have args. </p>
   */
  public void error(ErrorCodeEnum errorCode) {
    error(errorCode, null);
  }

  /**
   * This function adds an agent error level customer log, and it is also a datasource error.
   */
  public void error(String databaseType, String dataSourceErrorMessage, ErrorCodeEnum errorCode, ImmutableMap<String, Object> params) {
    if (!customerLogFlag || isEmpty()) {
      return;
    }

    String key = CustomerLogMessagesEnum.AGENT_DATASOURCE_ERROR.getKey();
    Document entity  = newErrorFatalLogDocument(key, errorCode);

    // params inner document
    Document paramsDoc = newJobLogParamsDocument(params);
    paramsDoc.put("errorCode", errorCode.getCode());
    paramsDoc.put("datasource", databaseType);
    paramsDoc.put("dataSourceErrorMessage", dataSourceErrorMessage);
    entity.put("params", paramsDoc);
    if (params != null) {
      entity.put("templateKeys", Collections.singletonList("errorMessage"));
    }

    saveToDb(entity);
  }

  /**
   * This function adds an agent error level customer log, and it is also a datasource error.
   */
  public void error(Connections connections, String dataSourceErrorMessage, ErrorCodeEnum errorCode, ImmutableMap<String, Object> params) {
    ImmutableMap.Builder<String, Object> builder =  ImmutableMap.builder();
    builder.put("dataSourceInfo", connections.getDataSourceInfo());
    for (String key : params.keySet()) {
      builder.put(key, params.get(key));
    }
    error(connections.getDatabase_type(), dataSourceErrorMessage, errorCode, builder.build());
  }

  /**
   * This function adds an agent error level customer log, and it is also a datasource error.
   *
   * <p> Same as {@code error(String databaseType, String dataSourceErrorMessage,
   * ErrorCodeEnum errorCode, ImmutableMap<String, Object> params)},  the difference is that in
   * this function params are not needed since some agent error level customer logs do not have
   * args. </p>
   */
  public void error(String databaseType, String dataSourceErrorMessage, ErrorCodeEnum errorCode) {
    error(databaseType, dataSourceErrorMessage, errorCode, null);
  }

  public void initialSyncStarted(String sourceDataSourceInfo, String targetDataSourceInfo) {
    info(CustomerLogMessagesEnum.AGENT_INITIAL_SYNC_STARTED, ImmutableMap.of(
      "sourceDataSourceInfo", sourceDataSourceInfo,
      "targetDataSourceInfo", targetDataSourceInfo
    ));
  }

  public void initialSyncStopped(String sourceDataSourceInfo, String targetDataSourceInfo) {
    info(CustomerLogMessagesEnum.AGENT_INITIAL_SYNC_STOPPED, ImmutableMap.of(
      "sourceDataSourceInfo", sourceDataSourceInfo,
      "targetDataSourceInfo", targetDataSourceInfo
    ));
  }

  public void initialSyncCompleted(String sourceDataSourceInfo, String targetDataSourceInfo, long seconds) {
    info(CustomerLogMessagesEnum.AGENT_INITIAL_SYNC_COMPLETED, ImmutableMap.of(
      "sourceDataSourceInfo", sourceDataSourceInfo,
      "targetDataSourceInfo", targetDataSourceInfo,
      "seconds", seconds
    ));
  }

  public void cdcSyncPrepareStart(String sourceDataSourceInfo, String targetDataSourceInfo) {
    info(CustomerLogMessagesEnum.AGENT_CDC_SYNC_PREPARE_START, ImmutableMap.of(
      "sourceDataSourceInfo", sourceDataSourceInfo,
      "targetDataSourceInfo", targetDataSourceInfo
    ));
  }

  public void cdcSyncStarted(String sourceDataSourceInfo, String targetDataSourceInfo, double seconds) {
    info(CustomerLogMessagesEnum.AGENT_CDC_SYNC_STARTED, ImmutableMap.of(
      "sourceDataSourceInfo", sourceDataSourceInfo,
      "targetDataSourceInfo", targetDataSourceInfo,
      "seconds", seconds
    ));
  }

  public void cdcSyncStopped(String sourceDataSourceInfo, String targetDataSourceInfo) {
    info(CustomerLogMessagesEnum.AGENT_CDC_SYNC_STOPPED, ImmutableMap.of(
      "sourceDataSourceInfo", sourceDataSourceInfo,
      "targetDataSourceInfo", targetDataSourceInfo
    ));
  }
}
