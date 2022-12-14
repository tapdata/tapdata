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

	PIPE_CLUSTER("pipeCluster");

	private String type;

	MessageType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
