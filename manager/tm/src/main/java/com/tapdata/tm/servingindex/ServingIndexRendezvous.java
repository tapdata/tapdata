package com.tapdata.tm.servingindex;

import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.LongSupplier;

/**
 * P3-3 · 落地腿的读回会合点。TAP-12057 · <b>ADR-0012 D1</b>。
 *
 * <p>CICD 落地这条腿没有前端 ws 会话，而比对在 TM ——<b>TM 自己就是读回结果的消费者</b>，
 * 而平台的 pipe 通道没有「回给 TM」这个地址。本类利用三条既有事实凑出一条会合点：</p>
 *
 * <ol>
 *   <li>引擎回推地址 = TM 发 pipe 时填的 {@code sender}（{@code ManagementWebsocketHandler}）；</li>
 *   <li>投递不到的 pipe 会被存进 {@code MessageQueue} 集合（{@code MessageQueueServiceImpl}）；</li>
 *   <li>{@code MessageQueueWatch} 只中继「本节点已连会话」的 receiver，不会截胡一个没有会话的 token。</li>
 * </ol>
 *
 * <p>于是：用 {@link #newToken()} 造一个<b>绝不属于任何会话</b>的地址当 sender，引擎的回推就会安静地
 * 落在库里等 {@link #await} 来取。跨 TM 节点成立（任何节点都查同一集合），这正是 ADR-0009「关联态 per-JVM
 * 静态」那条 HA 约束否掉内存 future 的地方。</p>
 *
 * <p><b>取到即删</b>：这个集合今天没有任何清理逻辑（无 TTL、无 delete），不删就是往里堆垃圾。</p>
 *
 * <p><b>迁移余地</b>：将来若改走「引擎回调 TM REST」（ADR-0012 Alternatives O2），只换本类实现，
 * 上层 {@link ServingIndexLandingService} 不认识 {@code MessageQueue}。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：本类只经 {@link MessageQueueService} 读写<b>平台库</b>的消息队列
 * （消息本就是平台数据），不依赖 {@code MongoTemplate}、不碰用户库。</p>
 */
@Component
@Slf4j
public class ServingIndexRendezvous {

	/** token 前缀：既表明用途，也确保不与 agentId / Spring session id 撞（撞了会被 Watch 中继走）。 */
	static final String TOKEN_PREFIX = "tm-si-landing:";

	private static final long DEFAULT_POLL_INTERVAL_MILLIS = 200L;

	/** 可注入的 sleep 接缝——让超时用例不靠真实等待。 */
	@FunctionalInterface
	interface Sleeper {
		void sleep(long millis) throws InterruptedException;
	}

	private final MessageQueueService messageQueueService;
	private final LongSupplier clock;
	private final Sleeper sleeper;
	private final long pollIntervalMillis;

	public ServingIndexRendezvous(MessageQueueService messageQueueService) {
		this(messageQueueService, System::currentTimeMillis, Thread::sleep, DEFAULT_POLL_INTERVAL_MILLIS);
	}

	ServingIndexRendezvous(MessageQueueService messageQueueService, LongSupplier clock, Sleeper sleeper,
						   long pollIntervalMillis) {
		this.messageQueueService = messageQueueService;
		this.clock = clock;
		this.sleeper = sleeper;
		this.pollIntervalMillis = pollIntervalMillis;
	}

	/** 造一个「没有会话」的回推地址；每次调用都不同，一次读回一个。 */
	public String newToken() {
		return TOKEN_PREFIX + UUID.randomUUID();
	}

	/**
	 * 等目标集合的索引读回结果。
	 *
	 * @param token         发 pipe 时用的 sender（= 引擎回推的 receiver）
	 * @param reqId         关联键（ADR-0009 KEPT）；同 token 上也校验它，避免吃到别人的应答
	 * @param timeoutMillis 上限；到点仍无结果按超时处理，交 P3-5 汇总
	 */
	public ServingIndexReadback await(String token, String reqId, long timeoutMillis) {
		Map<String, Object> data = consume(token, reqId, timeoutMillis);
		if (data == null) {
			return ServingIndexReadback.timeout();
		}
		String error = errorOf(data);
		if (error != null) {
			return ServingIndexReadback.failed(error);
		}
		Map<String, Object> result = asMap(data.get("result"));
		return ServingIndexReadback.success(result == null ? null : toIndexes(result.get("indexes")));
	}

	/**
	 * 等建索引回执（{@code createIndexResult}）。与读回同一条会合点、同一套关联口径。
	 *
	 * <p>回执要等，不能发完就算完：连接器会把 errorCode 85/86 catch 后 continue，「发出去了」不等于
	 * 「建成了」——逐条的 created/failed 只有回执里有（ADR-0005 / P3-2 的第二道兜底口径）。</p>
	 */
	public ServingIndexCreateAck awaitCreateAck(String token, String reqId, long timeoutMillis) {
		Map<String, Object> data = consume(token, reqId, timeoutMillis);
		if (data == null) {
			return ServingIndexCreateAck.timeout();
		}
		String error = errorOf(data);
		if (error != null) {
			return ServingIndexCreateAck.failed(error);
		}
		Map<String, Object> result = asMap(data.get("result"));
		return ServingIndexCreateAck.of(created(result), failed(result));
	}

	/** 轮询到本 token 上 reqId 匹配的应答，取到即删并返回其 {@code data}；超时返回 {@code null}。 */
	private Map<String, Object> consume(String token, String reqId, long timeoutMillis) {
		long deadline = clock.getAsLong() + timeoutMillis;
		while (true) {
			MessageQueueDto hit = poll(token, reqId);
			if (hit != null) {
				// 先删再解析：解析失败也不能把文档留在库里反复被捞（本集合无人清理）。
				messageQueueService.deleteById(hit.getId());
				return asMap(hit.getData());
			}
			if (clock.getAsLong() >= deadline) {
				log.warn("serving index rendezvous timeout, token = {}, reqId = {}", token, reqId);
				return null;
			}
			try {
				sleeper.sleep(pollIntervalMillis);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return null;
			}
		}
	}

	private static String errorOf(Map<String, Object> data) {
		String error = asString(data.get("error"));
		return error == null || error.isEmpty() ? null : error;
	}

	private static List<String> created(Map<String, Object> result) {
		List<String> names = new ArrayList<>();
		Object raw = result == null ? null : result.get("created");
		if (raw instanceof List) {
			for (Object name : (List<?>) raw) {
				names.add(asString(name));
			}
		}
		return names;
	}

	/** {@code failed:[{name, error}]} → {@code "名: 原因"}，逐条可归因（P3-5 汇总要用）。 */
	private static List<String> failed(Map<String, Object> result) {
		List<String> reasons = new ArrayList<>();
		Object raw = result == null ? null : result.get("failed");
		if (raw instanceof List) {
			for (Object element : (List<?>) raw) {
				Map<String, Object> entry = asMap(element);
				if (entry != null) {
					reasons.add(asString(entry.get("name")) + ": " + asString(entry.get("error")));
				}
			}
		}
		return reasons;
	}

	/** 捞本 token 上 reqId 匹配的那一条；对不上的是别人的应答，原样留着。 */
	private MessageQueueDto poll(String token, String reqId) {
		List<MessageQueueDto> candidates = messageQueueService
				.findAll(Query.query(Criteria.where("receiver").is(token)));
		if (candidates == null) {
			return null;
		}
		for (MessageQueueDto candidate : candidates) {
			Map<String, Object> result = resultOf(candidate);
			if (result != null && reqId != null && reqId.equals(result.get("reqId"))) {
				return candidate;
			}
		}
		return null;
	}

	private static Map<String, Object> resultOf(MessageQueueDto message) {
		Map<String, Object> data = asMap(message == null ? null : message.getData());
		return data == null ? null : asMap(data.get("result"));
	}

	/**
	 * 引擎侧 {@code List<TapIndex>} 经 Jackson + Mongo 往返后的形状 → {@link TapIndex}。
	 *
	 * <p>逐字段显式映射，<b>不</b>用 Jackson 反绑 {@code TapIndex}：它有 {@code getUnique()}/{@code isUnique()}
	 * 这类异形访问器，反序列化行为不稳（P3-2 已因同一理由在引擎侧改用显式映射）。字段名以
	 * {@code TapIndex}/{@code TapIndexField} 的 getter 为准：{@code indexFields} / {@code fieldAsc}。</p>
	 */
	private static List<TapIndex> toIndexes(Object raw) {
		List<TapIndex> indexes = new ArrayList<>();
		if (!(raw instanceof List)) {
			return indexes;
		}
		for (Object element : (List<?>) raw) {
			Map<String, Object> map = asMap(element);
			if (map == null) {
				continue;
			}
			TapIndex index = new TapIndex().name(asString(map.get("name")));
			Object unique = map.get("unique");
			if (unique instanceof Boolean) {
				index.unique((Boolean) unique);
			}
			Object fields = map.get("indexFields");
			if (fields instanceof List) {
				for (Object f : (List<?>) fields) {
					Map<String, Object> field = asMap(f);
					if (field == null) {
						continue;
					}
					TapIndexField indexField = new TapIndexField().name(asString(field.get("name")));
					Object asc = field.get("fieldAsc");
					if (asc instanceof Boolean) {
						indexField.fieldAsc((Boolean) asc);
					}
					index.indexField(indexField);
				}
			}
			indexes.add(index);
		}
		return indexes;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> asMap(Object value) {
		return value instanceof Map ? (Map<String, Object>) value : null;
	}

	private static String asString(Object value) {
		return value == null ? null : String.valueOf(value);
	}
}
