package io.tapdata.modules.api.net.error;

public interface NetErrors {
	int ERROR_ENCODER_NOT_FOUND = 8000;
	int JAVA_CUSTOM_DESERIALIZE_FAILED = 8001;
	int ENCODE_NOT_SUPPORTED = 8002;
	int RESURRECT_DATA_NULL = 8003;
	int ID_TYPE_NOT_FOUND = 8004;
	int GATEWAY_SESSION_HANDLER_CLASS_NEW_FAILED = 8005;
	int WEBSOCKET_SERVER_START_FAILED = 8006;
	int WEBSOCKET_LOGIN_FAILED = 8007;
	int WEBSOCKET_PROTOCOL_ILLEGAL = 8008;
	int WEBSOCKET_SSL_FAILED = 8009;
	int WEBSOCKET_URL_ILLEGAL = 8010;
	int WEBSOCKET_CONNECT_FAILED = 8011;
	int PERSISTENT_FAILED = 8012;
	int ILLEGAL_ENCODE = 8013;
	int CURRENT_NODE_ID_NOT_FOUND = 8014;
	int RESULT_IS_NULL = 8015;
	int QUEUE_IS_FULL = 8016;
 	int COMMAND_RECEIVED_ILLEGAL = 8017;
	int PDK_NOT_SUPPORT_COMMAND_CALLBACK = 8018;
	int ILLEGAL_PARAMETERS = 8019;
	int ENGINE_CHANNEL_OFFLINE = 8020;
	int NO_AVAILABLE_ENGINE = 8021;
	int MISSING_COMMAND_INFO = 8022;
	int CONSUME_COMMAND_RESULT_FAILED = 8023;
	int NO_WAITING_COMMAND = 8024;
	int COMMAND_EXECUTE_FAILED = 8025;
	int COMMAND_RESULT_CODE_MISSING = 8026;
	int UNKNOWN_ERROR = 8027;
	int PDK_HASH_NOT_FOUND = 8028;
	int UNEXPECTED_MONGO_OPERATOR = 8029;
	int CONNECTIONS_NOT_FOUND = 8030;
	int NO_FUNCTION_ON_TYPE = 8031;
	int RECEIVER_GENERIC_TYPES_ILLEGAL = 8032;
	int UNSUPPORTED_ENCODE = 8033;
	int ILLEGAL_STATE = 8034;
	int NODE_POST_HTTP_CODE = 8035;
	int NO_WORKABLE_IP = 8036;
	int RESULT_CODE_FAILED = 8037;
	int ERROR_METHOD_MAPPING_INVOKE_UNKNOWN_ERROR = 8038;
	int ERROR_RPC_ENCODER_NULL = 8039;
	int ERROR_METHODREQUEST_CRC_ILLEGAL = 8040;
	int ERROR_METHODREQUEST_SERVICE_NULL = 8041;
	int ERROR_METHODREQUEST_SERVICE_NOTFOUND = 8042;
	int ERROR_METHODREQUEST_SKELETON_NULL = 8043;
	int ERROR_METHODREQUEST_METHODNOTFOUND = 8044;
	int ERROR_RPC_DECODE_FAILED = 8045;
	int ERROR_RPC_ENCODER_NOTFOUND = 8046;
	int ERROR_METHODMAPPING_METHOD_NULL = 8047;
	int ERROR_RPC_ENCODE_FAILED = 8048;
	int MISSING_SERVICE_CALLER = 8049;
	int SERVICE_CALLER_EXECUTE_FAILED = 8050;
	int SERVICE_CALLER_PARSE_FAILED = 8051;
	int COMMAND_INFO_PARSE_FAILED = 8052;
	int ENGINE_MESSAGE_CALL_LOCAL_FAILED = 8053;
	int MESSAGE_RESULT_PARSE_FAILED = 8054;
}