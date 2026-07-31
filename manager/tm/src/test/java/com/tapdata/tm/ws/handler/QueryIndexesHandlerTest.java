package com.tapdata.tm.ws.handler;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.servingindex.ServingIndexService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * P2-6 · 服务型索引读回的浏览器入口（{@link QueryIndexesHandler}）。
 *
 * <p>核心回归：转发给引擎的 {@code sender} 必须是 {@link WebSocketContext#getSender()}（TM 侧真实会话键），
 * <b>不是</b>前端在载荷里自报的任何 id——后者不在 TM 会话表里，引擎回推必然落空（ADR-0009 修订的由来）。</p>
 */
class QueryIndexesHandlerTest {

	private static final String SESSION_KEY = "spring-session-abc";
	private static final String CONN_ID = "6a38d5da6edb30d9ee2df4f9";
	private static final String USER_ID = "6a38d2786edb30d9ee2df4cb";

	private QueryIndexesHandler handler;
	private DataSourceService dataSourceService;
	private UserService userService;
	private ServingIndexService servingIndexService;

	@BeforeEach
	void setUp() {
		handler = new QueryIndexesHandler();
		dataSourceService = mock(DataSourceService.class);
		userService = mock(UserService.class);
		servingIndexService = mock(ServingIndexService.class);
		handler.setDataSourceService(dataSourceService);
		handler.setUserService(userService);
		handler.setServingIndexService(servingIndexService);
	}

	private WebSocketContext context(Map<String, Object> data) {
		MessageInfo messageInfo = new MessageInfo();
		messageInfo.setType("queryIndexes");
		messageInfo.setData(data);
		return new WebSocketContext("ws-session-id", SESSION_KEY, USER_ID, messageInfo);
	}

	private Map<String, Object> data(String connectionId, String tableName, String reqId) {
		Map<String, Object> data = new HashMap<>();
		if (null != connectionId) data.put("connectionId", connectionId);
		if (null != tableName) data.put("tableName", tableName);
		if (null != reqId) data.put("reqId", reqId);
		// 前端自报的 clientId 是历史包袱：即便带上，也绝不能被当成 sender。
		data.put("clientId", "browser-generated-uuid");
		return data;
	}

	@Test
	@DisplayName("转发用会话键作 sender（不用载荷里前端自报的 clientId），reqId 原样透传")
	void handleMessage_usesSessionKeyAsSender() throws Exception {
		DataSourceConnectionDto conn = new DataSourceConnectionDto();
		conn.setId(new ObjectId(CONN_ID));
		conn.setUserId(USER_ID);
		UserDetail user = mock(UserDetail.class);
		when(dataSourceService.findById(any(ObjectId.class))).thenReturn(conn);
		when(userService.loadUserById(any(ObjectId.class))).thenReturn(user);

		handler.handleMessage(context(data(CONN_ID, "mdm_policy", "r-1")));

		ArgumentCaptor<String> sender = ArgumentCaptor.forClass(String.class);
		verify(servingIndexService).sendQueryIndexes(eq(conn), eq("mdm_policy"), eq("r-1"), sender.capture(), eq(user));
		assertEquals(SESSION_KEY, sender.getValue(), "sender 必须是 TM 会话键，否则引擎回推路由不到本会话");
	}

	@Test
	@DisplayName("缺 connectionId：不转发，且即刻回发带关联键的 queryIndexesResult 错误包")
	void handleMessage_missingConnectionId_repliesErrorWithoutDispatch() throws Exception {
		try (MockedStatic<WebSocketManager> ws = mockStatic(WebSocketManager.class)) {
			handler.handleMessage(context(data(null, "mdm_policy", "r-2")));

			verify(servingIndexService, never()).sendQueryIndexes(any(), anyString(), anyString(), anyString(), any());
			ArgumentCaptor<String> msg = ArgumentCaptor.forClass(String.class);
			ws.verify(() -> WebSocketManager.sendMessage(eq(SESSION_KEY), msg.capture()));
			assertTrue(msg.getValue().contains("queryIndexesResult"), "错误包须是 queryIndexesResult");
			assertTrue(msg.getValue().contains("r-2"), "错误包须带 reqId，前端才能关联并即时报错");
		}
	}

	@Test
	@DisplayName("连接不存在：不转发，回发错误包")
	void handleMessage_connectionNotFound_repliesError() throws Exception {
		when(dataSourceService.findById(any(ObjectId.class))).thenReturn(null);

		try (MockedStatic<WebSocketManager> ws = mockStatic(WebSocketManager.class)) {
			handler.handleMessage(context(data(CONN_ID, "mdm_policy", "r-3")));

			verify(servingIndexService, never()).sendQueryIndexes(any(), anyString(), anyString(), anyString(), any());
			ws.verify(() -> WebSocketManager.sendMessage(eq(SESSION_KEY), anyString()));
		}
	}
}
