package com.tapdata.tm.servingindex;

/**
 * P1-2 · 「按连接 + 表读回索引」触发端点请求体（TAP-12057 / ADR-0009）。
 *
 * <p><b>契约为 provisional（P2 前端联调点）</b>：{@code clientId} = 发起方前端 ws 会话 id（引擎回推的
 * {@code receiver}，经 PipeHandler 中继回该会话）；{@code reqId} = 前端生成的关联键（引擎在结果里回显）。
 * 字段与端点路径以 P2 前端落盘时最终确认为准。</p>
 */
public class QueryIndexesRequest {

	/** 目标表名。 */
	private String tableName;
	/** 前端生成的关联键；引擎在 {@code queryIndexesResult} 里回显，前端按 connectionId+tableName+reqId 关联。 */
	private String reqId;
	/** 发起方前端 ws 会话 id；作为 pipe 的 {@code sender}，引擎回推结果的目标会话。 */
	private String clientId;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getReqId() {
		return reqId;
	}

	public void setReqId(String reqId) {
		this.reqId = reqId;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
}
