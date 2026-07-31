package io.tapdata.websocket.handler;

import com.tapdata.entity.Connections;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.index.TwoDbRedlineViolationException;
import io.tapdata.schema.TapTableMap;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * P1-1 · queryIndexes 连接运维动作 handler（{@link QueryIndexesHandler}）。
 *
 * <p>本测试只锚定<b>编排契约</b>：事件校验 → 调用读回封口（seam）→ 映射为
 * {@link WebSocketEventResult}（同步返回，由 {@code ManagementWebsocketHandler} 回发）。
 * 封口 {@code queryIndexes(Connections, String)} 内部的 PDK 节点构建/生命周期属于集成范畴、
 * 此处以 spy 打桩隔离；真正的读回逻辑已在 {@code PdkIndexServiceTest} 单测覆盖。</p>
 */
class QueryIndexesHandlerTest {

	private QueryIndexesHandler handler;
	private SendMessage sendMessage;

	@BeforeEach
	void setUp() {
		handler = spy(new QueryIndexesHandler());
		sendMessage = mock(SendMessage.class);
	}

	private Map<String, Object> event(String tableName, String connId) {
		Map<String, Object> event = new HashMap<>();
		if (null != tableName) {
			event.put("tableName", tableName);
		}
		if (null != connId) {
			Map<String, Object> conn = new HashMap<>();
			conn.put("id", connId);
			conn.put("name", "conn-" + connId);
			event.put("connections", conn);
		}
		return event;
	}

	private TapIndex index(String name, String field, boolean asc) {
		TapIndexField f = new TapIndexField();
		f.setName(field);
		f.setFieldAsc(asc);
		TapIndex i = new TapIndex();
		i.setName(name);
		i.setIndexFields(new ArrayList<>(Collections.singletonList(f)));
		return i;
	}

	@Test
	@DisplayName("连接级动作用空表映射建节点：不得按 nodeId 查 TM 的 node/tableMap（那只认任务 DAG 节点）")
	void connectionScopedTableMap_isEmptyAndDoesNotHitTm() {
		Connections conn = new Connections();
		conn.setId("6a38d5da6edb30d9ee2df4f9"); // 连接 id，不是任务节点 id
		conn.setName("mongo");
		// 旧实现在此按 connectionId 走 TM /MetadataInstances/node/tableMap，
		// 真实环境返回 "taskDto is null" 系统错误、单测环境直接抛（无 ClientMongoOperator bean）。
		TapTableMap<String, TapTable> tableMap = handler.connectionScopedTableMap(conn);
		assertNotNull(tableMap);
		assertTrue(tableMap.isEmpty(), "连接级动作无任务上下文，表映射应为空；目标表由 queryIndexes 显式传入");
	}

	@Test
	@DisplayName("红线：目标解析到平台自有库 → queryIndexes 建节点前响亮失败（ADR-0002）")
	void queryIndexes_targetResolvesToPlatformDb_throwsRedline() throws Throwable {
		String platform = "mongodb://mongo:27017/tapdata";
		doReturn(platform).when(handler).platformMongoUri();
		Connections conn = new Connections();
		conn.setId("c1");
		conn.setName("evil");
		conn.setDatabase_uri(platform); // 指向平台自有库
		assertThrows(TwoDbRedlineViolationException.class, () -> handler.queryIndexes(conn, "orders"));
	}

	@Test
	@DisplayName("handle 成功：返回 queryIndexesResult 成功结果，并按解析出的表调用读回")
	void handle_returnsSuccessResultWithIndexes() throws Throwable {
		TapIndex a = index("idx_a", "a", true);
		doReturn(Collections.singletonList(a)).when(handler).queryIndexes(any(Connections.class), eq("orders"));

		WebSocketEventResult result = (WebSocketEventResult) handler.handle(event("orders", "c1"), sendMessage);

		assertNotNull(result);
		assertEquals(WebSocketEventResult.Type.QUERY_INDEXES_RESULT.getType(), result.getType());
		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_SUCCESS, result.getStatus());
		assertNotNull(result.getResult());
		verify(handler).queryIndexes(any(Connections.class), eq("orders"));
	}

	@Test
	@DisplayName("handle 回显 reqId：成功结果 payload 带上事件里的 reqId（ADR-0009 关联键）")
	void handle_echoesReqIdIntoSuccessPayload() throws Throwable {
		doReturn(Collections.singletonList(index("idx_a", "a", true)))
				.when(handler).queryIndexes(any(Connections.class), eq("orders"));
		Map<String, Object> ev = event("orders", "c1");
		ev.put("reqId", "r-123");

		WebSocketEventResult result = (WebSocketEventResult) handler.handle(ev, sendMessage);

		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_SUCCESS, result.getStatus());
		QueryIndexesHandler.QueryIndexesResult payload = (QueryIndexesHandler.QueryIndexesResult) result.getResult();
		assertEquals("r-123", payload.getReqId());
		assertEquals("c1", payload.getConnectionId());
		assertEquals("orders", payload.getTableName());
	}

	@Test
	@DisplayName("handle 失败也带关联键：payload 回带 connectionId/tableName/reqId，否则前端只能干等超时")
	void handle_whenQueryThrows_failurePayloadCarriesCorrelationKeys() throws Throwable {
		doThrow(new RuntimeException("boom")).when(handler).queryIndexes(any(Connections.class), eq("orders"));
		Map<String, Object> ev = event("orders", "c1");
		ev.put("reqId", "r-err");

		WebSocketEventResult result = (WebSocketEventResult) handler.handle(ev, sendMessage);

		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR, result.getStatus());
		QueryIndexesHandler.QueryIndexesResult payload = (QueryIndexesHandler.QueryIndexesResult) result.getResult();
		assertNotNull(payload, "失败结果也须带 payload，前端才能按 reqId 关联并即时报错");
		assertEquals("r-err", payload.getReqId());
		assertEquals("c1", payload.getConnectionId());
		assertEquals("orders", payload.getTableName());
	}

	@Test
	@DisplayName("handle 缺 tableName：返回失败结果，且不触达连接器")
	void handle_blankTableName_returnsFailureWithoutQuerying() throws Throwable {
		WebSocketEventResult result = (WebSocketEventResult) handler.handle(event(null, "c1"), sendMessage);

		assertEquals(WebSocketEventResult.Type.QUERY_INDEXES_RESULT.getType(), result.getType());
		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR, result.getStatus());
		verify(handler, never()).queryIndexes(any(), anyString());
	}

	@Test
	@DisplayName("handle 缺 connections：返回失败结果")
	void handle_missingConnections_returnsFailure() throws Throwable {
		WebSocketEventResult result = (WebSocketEventResult) handler.handle(event("orders", null), sendMessage);

		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR, result.getStatus());
		verify(handler, never()).queryIndexes(any(), anyString());
	}

	@Test
	@DisplayName("handle 读取抛错：返回失败结果、错误信息含表名（响亮不静默）")
	void handle_whenQueryThrows_returnsFailure() throws Throwable {
		doThrow(new RuntimeException("boom")).when(handler).queryIndexes(any(Connections.class), eq("orders"));

		WebSocketEventResult result = (WebSocketEventResult) handler.handle(event("orders", "c1"), sendMessage);

		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR, result.getStatus());
		assertNotNull(result.getError());
		assertTrue(result.getError().contains("orders"));
	}
}
