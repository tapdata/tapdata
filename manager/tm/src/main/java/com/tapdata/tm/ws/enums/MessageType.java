/**
 * @title: MessageType
 * @description:
 * @author lk
 * @date 2021/9/18
 */
package com.tapdata.tm.ws.enums;

public enum MessageType {

	PING("ping"),
	PONG("pong"),
	TEST_CONNECTION("testConnection"),

	DOWNLOAD_CONNECTOR("downLoadConnector"),

	PIPE("pipe"),

	LOGS("logs"),

	WATCH("watch"),

	EDIT_FLUSH("editFlush"),

	NOTIFICATION("notification"),

	DATA_FLOW_INSIGHT("dataFlowInsight"),

	/** 数据同步 */
	DATA_SYNC("dataSync"),

	UNSUBSCRIBE("unsubscribe"),
	/** 模型推演 */
	TRANSFORMER_STATUS_PUSH("metadataTransformerProgress"),

	TRANSFORMER("deduceSchema"),
	/**  */
	LOADJAR("loadJar"),

	CREATETABLEDDL("createTableDDL"),

	AUTO_INSPECT_AGAIN("autoInspectAgain"),
	TEST_RUN("testRun"),

	PIPE_CLUSTER("pipeCluster"),
	TEST_EXTERNAL_STORAGE("testExternalStorage"),
	DROP_TABLE("dropTable"),
	/** 服务型索引读回（TAP-12057 / ADR-0009）：浏览器经 ws 发起，sender 由 TM 填真实会话键 */
	QUERY_INDEXES("queryIndexes");

	private String type;

	MessageType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
