package com.hazelcast.persistence.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceHttpConfig;
import com.hazelcast.persistence.http.entity.IMapEntity;
import com.hazelcast.persistence.http.entity.LoginResp;
import com.hazelcast.persistence.http.entity.ResponseBody;
import com.hazelcast.persistence.http.entity.TMRequestException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * 引擎 IT 补丁类：hazelcast-persistence 5.5.0-SNAPSHOT 的 HttpTMIMap 编译于 spring 6 时代，
 * 其静态字段 {@code headers} 指向匿名 HttpHeaders 子类（{@code HttpTMIMap$1}）。spring 7 中
 * {@code HttpHeaders} 不再 implements {@code MultiValueMap}，加载该匿名类时抛
 * IncompatibleClassChangeError，导致 DRS/cloud 模式下任务状态存储初始化失败。
 * <p>
 * 本类与 jar 内同名类共存，编译进 test-classes 后优先于依赖 jar 加载，行为等价：
 * ①headers 改为普通 HttpHeaders 实例（不再继承 HttpHeaders）；②HttpEntity 构造走
 * {@code (T, HttpHeaders)} 重载；③其余 MapStore/登录/TM REST 协议与 jar 版完全一致
 * （users/generatetoken 登录，HazelcastPersistence 集合的 findOne/upsertWithWhere/deleteAll）。
 */
public class HttpTMIMap extends HttpIMap<PersistenceHttpConfig, HttpResource> {

	private static final HttpHeaders headers = new HttpHeaders();

	private LoginResp loginResp;

	private String accessCode;

	private PersistenceHttpConfig persistenceHttpConfig;

	private HttpResource httpResource;

	static {
		headers.add("Content-Type", "application/json");
	}

	public HttpTMIMap() {
	}

	public void doInit(PersistenceHttpConfig config, HttpResource resource) {
		super.doInit(config, resource);
		persistenceHttpConfig = config;
		httpResource = resource;
		accessCode = config.getAccessCode();
		if (StringUtils.isBlank(accessCode)) {
			throw new IllegalArgumentException("Access code cannot be empty");
		}
	}

	public void doClear() {
		deleteAll(null);
	}

	public void doDestroy() {
		destroy();
	}

	public void destroy() {
		Optional.ofNullable(httpResource).ifPresent(resource ->
				CommonUtils.ignoreAnyError(() -> resource.close()));
	}

	// ==================== MapStore ====================

	public Object load(String key) {
		validateToken();
		Map<String, Object> query = filterQuery(keyQuery(key));
		IMapEntity entity = findOne(query, new TypeReference<IMapEntity>() {
		});
		return entity == null ? null : entity.getData();
	}

	public Map<String, Object> loadAll(Collection<String> keys) {
		validateToken();
		Map<String, Object> query = filterQuery(keysQuery(keys));
		List<IMapEntity> entities = find(query, new TypeReference<IMapEntity>() {
		});
		if (entities == null) {
			return null;
		}
		Map<String, Object> result = new HashMap<>();
		for (IMapEntity entity : entities) {
			result.put(entity.getKey(), entity.getData());
		}
		return result;
	}

	public Iterable<String> loadAllKeys() {
		validateToken();
		Map<String, Object> query = filterQuery(imapQuery());
		List<IMapEntity> entities = find(query, new TypeReference<IMapEntity>() {
		});
		List<String> keys = new ArrayList<>();
		if (entities != null) {
			entities.forEach(entity -> keys.add(entity.getKey()));
		}
		return new HttpTMIterable(keys);
	}

	public void store(String key, Object value) {
		validateToken();
		Map<String, Object> where = whereQuery(keyQuery(key));
		IMapEntity entity = IMapEntity.create().imap(mapName).key(key)
				.value(getValue(value)).serializeAndCompressValue();
		upsert(where, new HttpEntity<>(entity, headers));
	}

	private static Map<String, Object> getValue(Object value) {
		Map<String, Object> data = new HashMap<>();
		data.put("_ts", System.currentTimeMillis() / 1000);
		data.put("value", value);
		return data;
	}

	public void storeAll(Map<String, Object> map) {
		map.forEach(this::store);
	}

	public void delete(String key) {
		validateToken();
		if (key == null) {
			return;
		}
		Map<String, Object> where = whereQuery(keyQuery(key));
		delete(where);
	}

	public void deleteAll(Collection<String> keys) {
		validateToken();
		Map<String, Object> query = imapQuery();
		if (keys != null) {
			query.put("key", inQuery(keys));
		}
		Map<String, Object> where = whereQuery(query);
		delete(where);
	}

	// ==================== 登录 ====================

	private void validateToken() {
		if (loginResp == null) {
			refreshToken();
		} else {
			long expiredTimestamp = loginResp.getExpiredTimestamp();
			if (expiredTimestamp - System.currentTimeMillis() <= 86400000L) {
				refreshToken();
			}
		}
	}

	private void refreshToken() {
		httpResource.retryWrap(retryInfo -> {
			Map<String, Object> params = new HashMap<>();
			params.put("accesscode", accessCode);
			HttpEntity<Map<String, Object>> entity = new HttpEntity<>(params, headers);
			URI uri = retryInfo.getURI(Resource.USER_GENERATE_TOKEN.getResource());
			try {
				loginResp = post(uri, entity, new TypeReference<LoginResp>() {
				});
			} catch (Throwable e) {
				if (e instanceof TMRequestException) {
					throw (TMRequestException) e;
				}
				throw new TMRequestException(String.format("Request uri[%s] failed, error: %s\n Request: %s",
						uri, e.getMessage(), entity), e);
			}
			if (loginResp == null) {
				throw new RuntimeException(String.format("Login response is null, uri: %s", uri));
			}
			loginResp.calcExpiredTimestamp();
			return false;
		}, null);
	}

	// ==================== TM REST 协议 ====================

	protected boolean successResp(ResponseEntity<ResponseBody> resp) {
		return resp != null && resp.hasBody() && resp.getStatusCode().is2xxSuccessful()
				&& ResponseCode.SUCCESS.getCode().equals(resp.getBody().getCode());
	}

	protected <E> E post(URI uri, HttpEntity<?> entity, TypeReference<E> typeRef) {
		ResponseEntity<ResponseBody> resp = httpResource.getRestTemplate()
				.exchange(uri, HttpMethod.POST, entity, ResponseBody.class);
		if (successResp(resp)) {
			Object data = resp.getBody().getData();
			return data == null ? null : JacksonUtil.convertValue(data, typeRef);
		}
		throw new TMRequestException(String.format("Request post[%s] failed\n Request: %s\n Response: %s",
				uri, entity, resp));
	}

	protected void upsert(Map<String, Object> where, HttpEntity<IMapEntity> entity) {
		httpResource.retryWrap(retryInfo -> {
			URI uri = retryInfo.getURI(where,
					Resource.HAZELCAST_PERSISTENCE.getResource(), Resource.UPSERT_WITH_WHERE.getResource());
			ResponseEntity<ResponseBody> resp = httpResource.getRestTemplate()
					.exchange(uri, HttpMethod.POST, entity, ResponseBody.class);
			if (!successResp(resp)) {
				throw new TMRequestException(String.format("Request upsert[%s] failed\n Request: %s\n Response: %s",
						uri, entity, resp));
			}
			return false;
		}, null);
	}

	protected <E> List<E> find(Map<String, Object> where, TypeReference<E> typeRef) {
		return httpResource.retryWrap(retryInfo -> {
			URI uri = retryInfo.getURI(where, Resource.HAZELCAST_PERSISTENCE.getResource());
			ResponseEntity<ResponseBody> resp = httpResource.getRestTemplate()
					.exchange(uri, HttpMethod.GET, null, ResponseBody.class);
			if (!successResp(resp)) {
				return null;
			}
			Object data = resp.getBody().getData();
			if (data instanceof Map && ((Map<?, ?>) data).containsKey("items")) {
				Object items = ((Map<?, ?>) data).get("items");
				if (items instanceof List) {
					List<E> result = new ArrayList<>();
					((List<?>) items).forEach(item -> result.add(JacksonUtil.convertValue(item, typeRef)));
					return result;
				}
				return null;
			}
			return null;
		}, null);
	}

	protected <E> E findOne(Map<String, Object> where, TypeReference<E> typeRef) {
		return httpResource.retryWrap(retryInfo -> {
			URI uri = retryInfo.getURI(where,
					Resource.HAZELCAST_PERSISTENCE.getResource(), Resource.FIND_ONE.getResource());
			ResponseEntity<ResponseBody> resp = httpResource.getRestTemplate()
					.exchange(uri, HttpMethod.GET, null, ResponseBody.class);
			if (!successResp(resp)) {
				return null;
			}
			Object data = resp.getBody().getData();
			return data == null ? null : JacksonUtil.convertValue(data, typeRef);
		}, null);
	}

	protected void delete(Map<String, Object> where) {
		httpResource.retryWrap(retryInfo -> {
			URI uri = retryInfo.getURI(where,
					Resource.HAZELCAST_PERSISTENCE.getResource(), Resource.DELETE_ALL.getResource());
			ResponseEntity<ResponseBody> resp = httpResource.getRestTemplate()
					.exchange(uri, HttpMethod.DELETE, null, ResponseBody.class);
			if (!successResp(resp)) {
				throw new TMRequestException(String.format("Request deleteAll[%s] failed\n Response: %s", uri, resp));
			}
			return false;
		}, null);
	}

	// ==================== 查询构造 ====================

	private Map<String, Object> imapQuery() {
		Map<String, Object> query = new HashMap<>();
		query.put("imap", mapName);
		return query;
	}

	private Map<String, Object> keyQuery(String key) {
		Map<String, Object> query = imapQuery();
		query.put("key", key);
		return query;
	}

	private Map<String, Object> keysQuery(Collection<String> keys) {
		Map<String, Object> query = imapQuery();
		query.put("key", inQuery(keys));
		return query;
	}

	private Map<String, Object> inQuery(Collection<String> keys) {
		Map<String, Object> query = new HashMap<>();
		query.put("$in", keys);
		return query;
	}

	private Map<String, Object> filterQuery(Map<String, Object> param) {
		Map<String, Object> where = new HashMap<>();
		try {
			where.put("where", JacksonUtil.toJson(param));
		} catch (JsonProcessingException ignored) {
		}
		Map<String, Object> filter = new HashMap<>();
		filter.put("filter", where);
		filter.put("access_token", loginResp.getId());
		return filter;
	}

	private Map<String, Object> whereQuery(Map<String, Object> param) {
		Map<String, Object> where = new HashMap<>();
		try {
			where.put("where", JacksonUtil.toJson(param));
		} catch (JsonProcessingException ignored) {
		}
		where.put("access_token", loginResp.getId());
		return where;
	}

	public PersistenceHttpConfig getPersistenceHttpConfig() {
		return persistenceHttpConfig;
	}

	// ==================== 内部类型 ====================

	enum Resource {
		USER_GENERATE_TOKEN("users/generatetoken"),
		HAZELCAST_PERSISTENCE("HazelcastPersistence"),
		FIND_ONE("findOne"),
		UPSERT_WITH_WHERE("upsertWithWhere"),
		DELETE_ALL("deleteAll");

		private final String resource;

		Resource(String resource) {
			this.resource = resource;
		}

		public String getResource() {
			return resource;
		}
	}

	enum ResponseCode {
		SUCCESS("ok");

		private final String code;

		ResponseCode(String code) {
			this.code = code;
		}

		public String getCode() {
			return code;
		}
	}

	public static class HttpTMIterable implements Iterable<String> {
		private List<String> keys;

		public HttpTMIterable(List<String> keys) {
			this.keys = keys;
			if (this.keys == null) {
				this.keys = new ArrayList<>();
			}
		}

		@Override
		public Iterator<String> iterator() {
			return keys.iterator();
		}

		@Override
		public void forEach(Consumer<? super String> action) {
			keys.forEach(action);
		}

		@Override
		public Spliterator<String> spliterator() {
			return keys.spliterator();
		}
	}
}
