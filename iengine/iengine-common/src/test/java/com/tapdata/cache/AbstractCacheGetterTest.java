package com.tapdata.cache;

import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AbstractCacheGetterTest {

	private static final String CACHE_NAME = "test-cache";

	private ICacheStore cacheStore;
	private ICacheStats cacheStats;

	private Map<String, Object> fullRow() {
		Map<String, Object> row = new HashMap<>();
		row.put("POLICY_ID", "P1");
		row.put("CUSTOMER_ID", "C1");
		row.put("CAR_MODEL", "M1");
		row.put("CREATE_TIME", "2026-01-01");
		row.put("REMARK", "should-not-be-cached");
		return row;
	}

	private DataFlowCacheConfig cacheConfig(Set<String> fields, List<String> primaryKeys) {
		DataFlowCacheConfig config = new DataFlowCacheConfig();
		config.setCacheName(CACHE_NAME);
		config.setFields(fields);
		config.setPrimaryKeys(primaryKeys);
		return config;
	}

	/**
	 * 测试用的 CacheGetter：record/recordList 恒为空以强制走 lookup，
	 * 并用注入的 rows 作为源库返回值。
	 */
	private AbstractCacheGetter newGetter(DataFlowCacheConfig config, List<Map<String, Object>> sourceRows) {
		IDataSourceRowsGetter rowsGetter = keys -> sourceRows;
		return new AbstractCacheGetter(config, cacheStore, cacheStats, null) {
			@Override
			protected List<Map<String, Object>> getRecordList(String cacheName, Object... cacheKeys) {
				return null;
			}

			@Override
			protected Map<String, Object> getRecord(String cacheName, Object... cacheKeys) {
				return null;
			}

			@Override
			public IDataSourceRowsGetter getDataSourceRowsGetter() {
				return rowsGetter;
			}
		};
	}

	@BeforeEach
	void setUp() {
		cacheStore = mock(ICacheStore.class);
		cacheStats = mock(ICacheStats.class);
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> captureCachedRows() throws Throwable {
		ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
		verify(cacheStore).cacheRow(anyString(), anyString(), captor.capture());
		return captor.getValue();
	}

	@Nested
	class GetAndSetCache {

		@Test
		void projectsSelectedFieldsAndPk() throws Throwable {
			Set<String> fields = new HashSet<>();
			fields.add("CUSTOMER_ID");
			fields.add("CAR_MODEL");
			DataFlowCacheConfig config = cacheConfig(fields, java.util.Collections.singletonList("POLICY_ID"));
			List<Map<String, Object>> sourceRows = new ArrayList<>();
			sourceRows.add(fullRow());

			AbstractCacheGetter getter = newGetter(config, sourceRows);
			Map<String, Object> result = getter.getAndSetCache(CACHE_NAME, true, "P1");

			List<Map<String, Object>> cached = captureCachedRows();
			assertEquals(1, cached.size());
			Set<String> keys = cached.get(0).keySet();
			assertTrue(keys.contains("CUSTOMER_ID"));
			assertTrue(keys.contains("CAR_MODEL"));
			assertFalse(keys.contains("POLICY_ID"));
			assertFalse(keys.contains("CREATE_TIME"));
			assertFalse(keys.contains("REMARK"));

			// 返回值同样只含投影后的字段
			assertFalse(result.containsKey("REMARK"));
		}

		@Test
		void keepsAllFieldsWhenNoFieldsConfigured() throws Throwable {
			DataFlowCacheConfig config = cacheConfig(new HashSet<>(), java.util.Collections.singletonList("POLICY_ID"));
			List<Map<String, Object>> sourceRows = new ArrayList<>();
			sourceRows.add(fullRow());

			AbstractCacheGetter getter = newGetter(config, sourceRows);
			getter.getAndSetCache(CACHE_NAME, true, "P1");

			List<Map<String, Object>> cached = captureCachedRows();
			assertTrue(cached.get(0).keySet().contains("REMARK"));
		}

		@Test
		void doesNotLookupWhenLookupFalse() throws Throwable {
			DataFlowCacheConfig config = cacheConfig(new HashSet<>(java.util.Arrays.asList("CUSTOMER_ID")),
					java.util.Collections.singletonList("POLICY_ID"));
			AbstractCacheGetter getter = newGetter(config, new ArrayList<>());
			getter.getAndSetCache(CACHE_NAME, false, "P1");
			verify(cacheStore, org.mockito.Mockito.never()).cacheRow(anyString(), anyString(), any());
		}
	}

	@Nested
	class GetAndSetCacheArray {

		@Test
		void projectsSelectedFieldsAndPk() throws Throwable {
			Set<String> fields = new HashSet<>();
			fields.add("CUSTOMER_ID");
			fields.add("CAR_MODEL");
			DataFlowCacheConfig config = cacheConfig(fields, java.util.Collections.singletonList("POLICY_ID"));
			List<Map<String, Object>> sourceRows = new ArrayList<>();
			sourceRows.add(fullRow());
			sourceRows.add(fullRow());

			AbstractCacheGetter getter = newGetter(config, sourceRows);
			List<Map<String, Object>> result = getter.getAndSetCacheArray(CACHE_NAME, true, "P1");

			List<Map<String, Object>> cached = captureCachedRows();
			assertEquals(2, cached.size());
			for (Map<String, Object> row : cached) {
				Set<String> keys = row.keySet();
				assertTrue(keys.contains("CUSTOMER_ID"));
				assertTrue(keys.contains("CAR_MODEL"));
				assertFalse(keys.contains("POLICY_ID"));
				assertFalse(keys.contains("CREATE_TIME"));
				assertFalse(keys.contains("REMARK"));
			}
			assertEquals(2, result.size());
			assertFalse(result.get(0).containsKey("REMARK"));
		}
	}
}
