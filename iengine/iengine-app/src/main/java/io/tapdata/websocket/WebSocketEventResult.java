package io.tapdata.websocket;

import com.tapdata.constant.Log4jUtil;

import java.io.Serializable;

/**
 * 消息处理结果
 *
 * @author jackin
 */
public class WebSocketEventResult implements Serializable {

	private static final long serialVersionUID = -4052014404649475245L;

	public static final String EVENT_HANDLE_RESULT_SUCCESS = "SUCCESS";

	public static final String EVENT_HANDLE_RESULT_ERRPR = "ERROR";

	/**
	 * 结果类型
	 */
	private String type;

	private String error;

	private String status;

	private Object result;

	private WebSocketEventResult() {
	}

	public static WebSocketEventResult handleSuccess(Type type, Object result) {
		WebSocketEventResult handleResult = new WebSocketEventResult();
		handleResult.setType(type.getType());
		handleResult.setStatus(EVENT_HANDLE_RESULT_SUCCESS);
		handleResult.setResult(result);

		return handleResult;
	}


	public static WebSocketEventResult handleFailed(Type type, String error) {
		WebSocketEventResult handleResult = new WebSocketEventResult();
		handleResult.setType(type.getType());
		handleResult.setStatus(EVENT_HANDLE_RESULT_ERRPR);
		handleResult.setError(error);

		return handleResult;
	}

	public static WebSocketEventResult handleFailed(Type type, String error, Throwable throwable) {
		WebSocketEventResult handleResult = new WebSocketEventResult();
		handleResult.setType(type.getType());
		handleResult.setStatus(EVENT_HANDLE_RESULT_ERRPR);
		if (throwable != null) {
			String stackString = Log4jUtil.getStackString(throwable);
			error += "\n" + stackString;
		}
		handleResult.setError(error);

		return handleResult;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public enum Type {
		EXECUTE_SCRIPT_RESULT("execute_script_result"),
		EXECUTE_LOAD_SCHEMA_RESULT("execute_load_schema_result"),
		UNKNOWN_EVENT_RESULT("unknown_event_result"),
		HANDLE_EVENT_ERROR_RESULT("handle_event_error_result"),
		EXECUTE_DATA_INSPECT_RESULT("execute_data_inspect_result"),
		SUBSCRIBE_EVENT_RESULT("subscribe_result"),
		AGGREGATE_PREVIEW_RESULT("aggregatePreviewResult"),
		TEST_CONNECTION_RESULT("testConnectionResult"),
		LOAD_JAR_LIB_RESULT("loadJarLibResult"),
		LOAD_VIKA_RESULT("loadVikaResult"),
		DATA_SYNC_RESULT("dataSyncResult"),

		DEDUCE_SCHEMA("deduceSchemaResult"),
		TEST_RUN("testRunResult"),
		AUTO_INSPECT_AGAIN("autoInspectAgainResult"),
		DROP_TABLE("dropTable"),
		DOWNLOAD_PDK_FILE_FLAG("downloadPdkFileFlag"),
		PROGRESS_REPORTING("progressReporting")
		;

		private String type;

		Type(String type) {
			this.type = type;
		}

		public String getType() {
			return type;
		}
	}

	@Override
	public String toString() {
		return "WebSocketEventResult{" +
				"type='" + type + '\'' +
				", error='" + error + '\'' +
				", status='" + status + '\'' +
				", result=" + result +
				'}';
	}
}
