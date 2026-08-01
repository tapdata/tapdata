package com.tapdata.tm.servingindex;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * P3-3 · 落地腿读回会合点（{@link ServingIndexRendezvous}）。[ADR-0012](../../../../../../../../adr/0012-landing-readback-rendezvous-and-conmap-key.md) D1。
 *
 * <p>CICD 落地腿没有前端会话，TM 自己是读回的消费者。引擎回推 {@code receiver=TM 自造 token} → 无会话命中
 * → 落 {@code MessageQueue} 集合 → 本类按 token 轮询取走。这里钉三样：<b>token 不与任何会话键碰撞</b>、
 * <b>取到即删</b>（今天没人清这个集合）、<b>线上形状</b>（`indexFields` 而非 `fields`——载荷形状猜错正是 P2 实机
 * 挖出的缺陷类型，故用真实 {@link TapIndex} 序列化→Map 往返来钉，而不是手抄一份 JSON）。</p>
 */
class ServingIndexRendezvousTest {

	private MessageQueueService messageQueueService;
	private AtomicLong now;
	private ServingIndexRendezvous rendezvous;

	@BeforeEach
	void setUp() {
		messageQueueService = mock(MessageQueueService.class);
		now = new AtomicLong(1_000L);
		// 时钟与 sleep 都是可注入的接缝：超时用例不靠真实等待，故不会因机器负载抖动。
		rendezvous = new ServingIndexRendezvous(messageQueueService, now::get, millis -> now.addAndGet(millis), 10L);
	}

	private static TapIndex tapIndex(String name, Boolean unique, String field, Boolean asc) {
		TapIndex index = new TapIndex().name(name);
		if (unique != null) {
			index.unique(unique);
		}
		return index.indexField(new TapIndexField().name(field).fieldAsc(asc));
	}

	/** 真实 TapIndex → Jackson → Map，模拟「引擎序列化 + Mongo 往返」后落在 `MessageQueueDto.data` 里的形状。 */
	private static Map<String, Object> reply(String reqId, String status, String error, TapIndex... indexes) {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("connectionId", "665f00000000000000000001");
		result.put("tableName", "CUSTOMER");
		result.put("reqId", reqId);
		result.put("indexes", JsonUtil.parseJsonUseJackson(
				JsonUtil.toJsonUseJackson(Arrays.asList(indexes)), new TypeReference<List<Object>>() {}));
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("type", "queryIndexesResult");
		data.put("status", status);
		if (error != null) {
			data.put("error", error);
		}
		data.put("result", result);
		return data;
	}

	private static MessageQueueDto doc(Map<String, Object> data) {
		MessageQueueDto dto = new MessageQueueDto();
		dto.setId(new ObjectId());
		dto.setType("pipe");
		dto.setReceiver("tm-si-landing:abc");
		dto.setData(data);
		return dto;
	}

	@Test
	@DisplayName("token 带专属前缀且每次不同——绝不与 agentId / Spring session id 碰撞（碰上会被 Watch 中继走）")
	void tokensAreNamespacedAndUnique() {
		String a = rendezvous.newToken();
		String b = rendezvous.newToken();
		assertTrue(a.startsWith("tm-si-landing:"), a);
		assertNotEquals(a, b);
	}

	@Test
	@DisplayName("取到匹配 reqId 的成功包 → 还原索引身份（有序字段+方向+unique），并把文档删掉")
	void consumesMatchingReplyAndDeletesIt() {
		MessageQueueDto hit = doc(reply("r-1", "SUCCESS", null, tapIndex("a_1", true, "a", true)));
		when(messageQueueService.findAll(any(Query.class))).thenReturn(Collections.singletonList(hit));

		ServingIndexReadback readback = rendezvous.await("tm-si-landing:abc", "r-1", 1_000L);

		assertFalse(readback.isTimedOut());
		assertNull(readback.getError());
		assertEquals(1, readback.getIndexes().size());
		TapIndex got = readback.getIndexes().get(0);
		assertEquals("a_1", got.getName());
		assertEquals(Boolean.TRUE, got.getUnique());
		assertEquals(1, got.getIndexFields().size());
		assertEquals("a", got.getIndexFields().get(0).getName());
		assertEquals(Boolean.TRUE, got.getIndexFields().get(0).getFieldAsc());
		verify(messageQueueService).deleteById(hit.getId());
	}

	@Test
	@DisplayName("线上形状钉死：方向 false 也要原样还原——方向丢了就是 P0 修的那个「读 -1 写 1」")
	void preservesDescendingDirection() {
		MessageQueueDto hit = doc(reply("r-1", "SUCCESS", null, tapIndex("b_-1", null, "b", false)));
		when(messageQueueService.findAll(any(Query.class))).thenReturn(Collections.singletonList(hit));

		ServingIndexReadback readback = rendezvous.await("tm-si-landing:abc", "r-1", 1_000L);

		assertEquals(Boolean.FALSE, readback.getIndexes().get(0).getIndexFields().get(0).getFieldAsc());
	}

	@Test
	@DisplayName("失败包照样消费：带出 error、索引空——不能让它当成「目标没有索引」")
	void consumesErrorReply() {
		MessageQueueDto hit = doc(reply("r-1", "ERROR", "Query indexes failed, table: CUSTOMER"));
		when(messageQueueService.findAll(any(Query.class))).thenReturn(Collections.singletonList(hit));

		ServingIndexReadback readback = rendezvous.await("tm-si-landing:abc", "r-1", 1_000L);

		assertFalse(readback.isTimedOut());
		assertEquals("Query indexes failed, table: CUSTOMER", readback.getError());
		assertTrue(readback.getIndexes().isEmpty());
		verify(messageQueueService).deleteById(hit.getId());
	}

	@Test
	@DisplayName("reqId 对不上的包不消费、不删——那是别人的应答")
	void ignoresForeignReqId() {
		MessageQueueDto other = doc(reply("r-other", "SUCCESS", null, tapIndex("a_1", null, "a", true)));
		when(messageQueueService.findAll(any(Query.class))).thenReturn(Collections.singletonList(other));

		ServingIndexReadback readback = rendezvous.await("tm-si-landing:abc", "r-1", 50L);

		assertTrue(readback.isTimedOut());
		verify(messageQueueService, never()).deleteById(any());
	}

	@Test
	@DisplayName("一直没来 → 超时（引擎不在线时 sendMessage 静默落库，结果永不到）")
	void timesOutWhenNothingArrives() {
		when(messageQueueService.findAll(any(Query.class))).thenReturn(Collections.emptyList());

		ServingIndexReadback readback = rendezvous.await("tm-si-landing:abc", "r-1", 50L);

		assertTrue(readback.isTimedOut());
		assertTrue(readback.getIndexes().isEmpty());
		verify(messageQueueService, never()).deleteById(any());
	}

	@Test
	@DisplayName("残缺载荷不炸：data 非 Map / 无 result / indexes 缺失，都按「拿不到」处理")
	void toleratesMalformedPayload() {
		MessageQueueDto noResult = doc(new LinkedHashMap<>());
		when(messageQueueService.findAll(any(Query.class))).thenReturn(Collections.singletonList(noResult));
		assertTrue(rendezvous.await("tm-si-landing:abc", "r-1", 20L).isTimedOut());

		MessageQueueDto notAMap = new MessageQueueDto();
		notAMap.setId(new ObjectId());
		notAMap.setData("not-a-map");
		when(messageQueueService.findAll(any(Query.class))).thenReturn(Collections.singletonList(notAMap));
		assertTrue(rendezvous.await("tm-si-landing:abc", "r-1", 20L).isTimedOut());

		Map<String, Object> emptyIndexes = reply("r-1", "SUCCESS", null);
		((Map<String, Object>) emptyIndexes.get("result")).remove("indexes");
		MessageQueueDto hit = doc(emptyIndexes);
		when(messageQueueService.findAll(any(Query.class))).thenReturn(new ArrayList<>(Collections.singletonList(hit)));
		ServingIndexReadback readback = rendezvous.await("tm-si-landing:abc", "r-1", 20L);
		assertFalse(readback.isTimedOut());
		assertTrue(readback.getIndexes().isEmpty());
	}
}
