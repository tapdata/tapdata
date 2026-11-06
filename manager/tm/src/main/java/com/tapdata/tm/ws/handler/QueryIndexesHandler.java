package com.tapdata.tm.ws.handler;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.servingindex.ServingIndexService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * P2-6 · 「服务型索引读回」的<b>浏览器入口</b>（TAP-12057 / ADR-0009 修订）。
 *
 * <p>为什么必须走 ws 而不是 REST：TM 的会话表按 {@code WebSocketServer#getId(session)} 建键
 * ——引擎是 {@code agentId}、浏览器是 Spring 的 {@code session.getId()}；连接 URL 上那个 {@code ?id=}
 * 前端自生成 uuid <b>不参与路由</b>。若由前端在 REST 请求体里自报 {@code clientId} 当 sender，引擎回推时
 * {@code WebSocketManager.containsSession} 必然落空，消息被存库、前端干等超时（2026-07-30 实机验证所见）。
 * 经 ws 发起则由本 handler 用 {@link WebSocketContext#getSender()}（= 真实会话键）作 sender，
 * 引擎以 {@code receiver=sender} 回推即天然命中——与 {@code testConnection}/{@code pipe} 同款。</p>
 *
 * <p>入参 {@code data}：{@code connectionId} + {@code tableName} + {@code reqId}（前端生成的关联键）。
 * 校验失败即刻回发 {@code queryIndexesResult} 错误包（带关联键），避免前端等满超时。</p>
 */
@WebSocketMessageHandler(type = MessageType.QUERY_INDEXES)
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class QueryIndexesHandler implements WebSocketHandler {

	static final String CONNECTION_ID = "connectionId";
	static final String TABLE_NAME = "tableName";
	static final String REQ_ID = "reqId";
	static final String RESULT_TYPE = "queryIndexesResult";

	private UserService userService;
	private DataSourceService dataSourceService;
	private ServingIndexService servingIndexService;

	@Override
	public void handleMessage(WebSocketContext context) throws Exception {
		MessageInfo messageInfo = context.getMessageInfo();
		Map<String, Object> data = null == messageInfo ? null : messageInfo.getData();
		String connectionId = value(data, CONNECTION_ID);
		String tableName = value(data, TABLE_NAME);
		String reqId = value(data, REQ_ID);

		if (StringUtils.isBlank(connectionId) || StringUtils.isBlank(tableName)) {
			replyError(context, connectionId, tableName, reqId, "connectionId and tableName cannot be empty");
			return;
		}

		DataSourceConnectionDto connectionDto = dataSourceService.findById(toObjectId(connectionId));
		if (null == connectionDto) {
			replyError(context, connectionId, tableName, reqId, "connection not found: " + connectionId);
			return;
		}

		String userId = StringUtils.isNotBlank(context.getUserId()) ? context.getUserId() : connectionDto.getUserId();
		UserDetail userDetail = StringUtils.isBlank(userId) ? null : userService.loadUserById(toObjectId(userId));
		if (null == userDetail) {
			replyError(context, connectionId, tableName, reqId, "UserDetail is null");
			return;
		}

		// sender = 本会话键：引擎回推 receiver=sender 时命中本浏览器会话（见类注释）。
		servingIndexService.sendQueryIndexes(connectionDto, tableName, reqId, context.getSender(), userDetail);
	}

	private String value(Map<String, Object> data, String key) {
		return null == data ? null : Objects.toString(data.get(key), null);
	}

	/** 回发与引擎失败包同形的 {@code pipe / queryIndexesResult} 错误（带关联键），前端即时报错而非等超时。 */
	private void replyError(WebSocketContext context, String connectionId, String tableName, String reqId, String error) {
		log.warn("query indexes rejected, sender = {}, error = {}", context.getSender(), error);
		Map<String, Object> payload = new HashMap<>();
		payload.put(CONNECTION_ID, connectionId);
		payload.put(TABLE_NAME, tableName);
		payload.put(REQ_ID, reqId);
		Map<String, Object> body = new HashMap<>();
		body.put("type", RESULT_TYPE);
		body.put("status", "ERROR");
		body.put("error", error);
		body.put("result", payload);
		Map<String, Object> envelope = new HashMap<>();
		envelope.put("type", MessageType.PIPE.getType());
		envelope.put("data", body);
		try {
			WebSocketManager.sendMessage(context.getSender(), com.tapdata.manager.common.utils.JsonUtil.toJson(envelope));
		} catch (Exception e) {
			log.warn("reply query indexes error failed, sender = {}, message = {}", context.getSender(), e.getMessage());
		}
	}
}
