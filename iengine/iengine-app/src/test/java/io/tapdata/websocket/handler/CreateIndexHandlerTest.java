package io.tapdata.websocket.handler;

import com.tapdata.entity.Connections;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.index.TwoDbRedlineViolationException;
import io.tapdata.schema.TapTableMap;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * P3 · createIndex 连接运维动作 handler（{@link CreateIndexHandler}）。
 *
 * <p>与读侧 {@code QueryIndexesHandler} 同构：事件校验 → 调用建索引封口（seam）→ 映射为
 * {@link WebSocketEventResult}。封口内的 PDK 节点构建/生命周期属集成范畴、以 spy 隔离；
 * 真正的下发已在 {@code PdkIndexServiceTest} 覆盖。本测试钉三件事：</p>
 * <ul>
 *   <li><b>方向忠实</b>——线上载荷 {@code asc:false} 必须映成 {@code fieldAsc=false}（P0 修复的意义所在）；</li>
 *   <li><b>不首错即停</b>——一条建失败，其余照建，逐条记账（P3-5）；</li>
 *   <li><b>两库红线</b>——目标解析到平台自有库则建节点前响亮失败（ADR-0002）。</li>
 * </ul>
 */
class CreateIndexHandlerTest {

	private CreateIndexHandler handler;
	private SendMessage sendMessage;

	@BeforeEach
	void setUp() {
		handler = spy(new CreateIndexHandler());
		sendMessage = mock(SendMessage.class);
	}

	private Map<String, Object> field(String name, Boolean asc) {
		Map<String, Object> f = new LinkedHashMap<>();
		f.put("field", name);
		if (null != asc) {
			f.put("asc", asc);
		}
		return f;
	}

	private Map<String, Object> spec(String name, Boolean unique, Map<String, Object>... fields) {
		Map<String, Object> spec = new HashMap<>();
		spec.put("name", name);
		if (null != unique) {
			spec.put("unique", unique);
		}
		spec.put("fields", new ArrayList<>(Arrays.asList(fields)));
		return spec;
	}

	private Map<String, Object> event(String tableName, String connId, List<Map<String, Object>> indexes) {
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
		if (null != indexes) {
			event.put("indexes", indexes);
		}
		return event;
	}

	private List<Map<String, Object>> oneIndex() {
		return new ArrayList<>(Arrays.asList(spec("a_1", null, field("a", true))));
	}

	@Test
	@DisplayName("载荷 → TapIndex：方向忠实映射，asc:false 必须成 fieldAsc=false（P0 语义）")
	void mapsDescendingDirectionFaithfully() {
		TapIndex index = CreateIndexHandler.toTapIndex(spec("a_1_b_-1", null, field("a", true), field("b", false)));

		assertEquals(2, index.getIndexFields().size());
		assertEquals("a", index.getIndexFields().get(0).getName());
		assertTrue(index.getIndexFields().get(0).getFieldAsc());
		assertEquals("b", index.getIndexFields().get(1).getName());
		assertFalse(index.getIndexFields().get(1).getFieldAsc(), "降序丢了就是 P0 那个坑的翻版");
	}

	@Test
	@DisplayName("载荷 → TapIndex：asc 缺省按升序（同 P0：null→1）")
	void missingAscMeansAscending() {
		TapIndex index = CreateIndexHandler.toTapIndex(spec("a_1", null, field("a", null)));

		assertTrue(index.getIndexFields().get(0).getFieldAsc());
	}

	@Test
	@DisplayName("载荷 → TapIndex：名字与 unique 照抄（名字由 TM 按字段集推导，unique 是创建参数）")
	void carriesNameAndUnique() {
		TapIndex index = CreateIndexHandler.toTapIndex(spec("a_1", true, field("a", true)));

		assertEquals("a_1", index.getName());
		assertTrue(index.isUnique());
	}

	@Test
	@DisplayName("不首错即停：一条建失败，其余照建，逐条记账（P3-5）")
	void collectsPerIndexFailuresAndKeepsGoing() {
		List<TapIndex> indexes = Arrays.asList(
				CreateIndexHandler.toTapIndex(spec("a_1", null, field("a", true))),
				CreateIndexHandler.toTapIndex(spec("bad_1", null, field("bad", true))),
				CreateIndexHandler.toTapIndex(spec("c_1", null, field("c", true))));
		CreateIndexHandler.CreateIndexResult result = new CreateIndexHandler.CreateIndexResult("c1", "orders");

		CreateIndexHandler.applyEach(indexes, index -> {
			if ("bad_1".equals(index.getName())) {
				throw new RuntimeException("boom");
			}
		}, result);

		assertEquals(Arrays.asList("a_1", "c_1"), result.getCreated(), "失败一条不该中断后续");
		assertEquals(1, result.getFailed().size());
		assertEquals("bad_1", result.getFailed().get(0).getName());
		assertTrue(result.getFailed().get(0).getError().contains("boom"));
	}

	@Test
	@DisplayName("handle 成功：返回 createIndexResult 成功结果，并按解析出的表调用建索引")
	void handleReturnsSuccessResult() throws Throwable {
		doAnswer(inv -> null).when(handler).createIndexes(any(Connections.class), eq("orders"), anyList(), any());

		WebSocketEventResult result = (WebSocketEventResult) handler.handle(event("orders", "c1", oneIndex()), sendMessage);

		assertNotNull(result);
		assertEquals(WebSocketEventResult.Type.CREATE_INDEX_RESULT.getType(), result.getType());
		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_SUCCESS, result.getStatus());
		verify(handler).createIndexes(any(Connections.class), eq("orders"), anyList(), any());
	}

	@Test
	@DisplayName("handle 回显 reqId：成功结果 payload 带关联键（ADR-0009）")
	void handleEchoesReqId() throws Throwable {
		doAnswer(inv -> null).when(handler).createIndexes(any(Connections.class), eq("orders"), anyList(), any());
		Map<String, Object> ev = event("orders", "c1", oneIndex());
		ev.put("reqId", "r-123");

		WebSocketEventResult result = (WebSocketEventResult) handler.handle(ev, sendMessage);

		CreateIndexHandler.CreateIndexResult payload = (CreateIndexHandler.CreateIndexResult) result.getResult();
		assertEquals("r-123", payload.getReqId());
		assertEquals("c1", payload.getConnectionId());
		assertEquals("orders", payload.getTableName());
	}

	@Test
	@DisplayName("handle 整体抛错也带关联键：payload 回带 connectionId/tableName/reqId，否则调用方只能干等超时")
	void failurePayloadCarriesCorrelationKeys() throws Throwable {
		doThrow(new RuntimeException("boom")).when(handler)
				.createIndexes(any(Connections.class), eq("orders"), anyList(), any());
		Map<String, Object> ev = event("orders", "c1", oneIndex());
		ev.put("reqId", "r-err");

		WebSocketEventResult result = (WebSocketEventResult) handler.handle(ev, sendMessage);

		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR, result.getStatus());
		CreateIndexHandler.CreateIndexResult payload = (CreateIndexHandler.CreateIndexResult) result.getResult();
		assertNotNull(payload, "失败结果也须带 payload");
		assertEquals("r-err", payload.getReqId());
		assertEquals("orders", payload.getTableName());
		assertTrue(result.getError().contains("orders"));
	}

	@Test
	@DisplayName("handle 缺 tableName：返回失败结果，且不触达连接器")
	void blankTableNameFailsWithoutTouchingConnector() throws Throwable {
		WebSocketEventResult result = (WebSocketEventResult) handler.handle(event(null, "c1", oneIndex()), sendMessage);

		assertEquals(WebSocketEventResult.Type.CREATE_INDEX_RESULT.getType(), result.getType());
		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR, result.getStatus());
		verify(handler, never()).createIndexes(any(), anyString(), anyList(), any());
	}

	@Test
	@DisplayName("handle 缺 connections：返回失败结果")
	void missingConnectionsFails() throws Throwable {
		WebSocketEventResult result = (WebSocketEventResult) handler.handle(event("orders", null, oneIndex()), sendMessage);

		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR, result.getStatus());
		verify(handler, never()).createIndexes(any(), anyString(), anyList(), any());
	}

	@Test
	@DisplayName("handle 无索引可建：响亮失败而非静默成功——调用方只在有『将创建』时才该下发")
	void missingIndexesFailsLoudly() throws Throwable {
		WebSocketEventResult result = (WebSocketEventResult) handler.handle(event("orders", "c1", null), sendMessage);

		assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR, result.getStatus());
		verify(handler, never()).createIndexes(any(), anyString(), anyList(), any());
	}

	@Test
	@DisplayName("红线：目标解析到平台自有库 → 建节点前响亮失败（ADR-0002）")
	void targetResolvingToPlatformDbIsRefused() {
		String platform = "mongodb://mongo:27017/tapdata";
		doAnswer(inv -> platform).when(handler).platformMongoUri();
		Connections conn = new Connections();
		conn.setId("c1");
		conn.setName("evil");
		conn.setDatabase_uri(platform);

		assertThrows(TwoDbRedlineViolationException.class, () -> handler.createIndexes(conn, "orders",
				Arrays.asList(CreateIndexHandler.toTapIndex(spec("a_1", null, field("a", true)))),
				new CreateIndexHandler.CreateIndexResult("c1", "orders")));
	}

	@Test
	@DisplayName("连接级动作用空表映射建节点：不得按 nodeId 查 TM 的 node/tableMap")
	void connectionScopedTableMapIsEmpty() {
		Connections conn = new Connections();
		conn.setId("6a38d5da6edb30d9ee2df4f9");
		conn.setName("mongo");

		TapTableMap<String, TapTable> tableMap = handler.connectionScopedTableMap(conn);

		assertNotNull(tableMap);
		assertTrue(tableMap.isEmpty(), "连接级动作无任务上下文；目标表由 createIndexes 显式传入");
	}
}
