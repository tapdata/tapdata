package com.tapdata.cache;

import com.tapdata.cache.external.ExternalStorageCacheGetter;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;

public class AbstractCacheGetterTest {

	private static final String CACHE_NAME = "test-cache";

	private ICacheStore cacheStore;

	private DataFlowCacheConfig cacheConfig(Set<String> fields, List<String> primaryKeys) {
		DataFlowCacheConfig config = new DataFlowCacheConfig();
		config.setCacheName(CACHE_NAME);
		config.setFields(fields);
		config.setPrimaryKeys(primaryKeys);
		return config;
	}

	@BeforeEach
	void setUp() {
		cacheStore = mock(ICacheStore.class);
	}

	@Nested
	class DataSourceRowsGetter {

		private ExternalStorageCacheGetter newExternalStorageGetter(DataFlowCacheConfig config) {
			try (MockedStatic<ExternalStorageUtil> externalStorageUtil = mockStatic(ExternalStorageUtil.class);
				 MockedConstruction<DocumentIMap> ignored = mockConstruction(DocumentIMap.class)) {
				externalStorageUtil.when(() -> ExternalStorageUtil.getExternalStorage((Node) any())).thenReturn(new ExternalStorageDto());
				return new ExternalStorageCacheGetter(cacheStore, config, null, null);
			}
		}

		@Test
		@DisplayName("TAP-13150: concurrent cache misses create a single data source connector")
		void concurrentMissesCreateSingleConnector() throws Exception {
			// PdkDataSourceRowsGetter 构造即初始化连接器（MongoDB 连接器新建 MongoClient），
			// 并发未命中重复创建时只有最后一个被保留，其余永远不会关闭
			ExternalStorageCacheGetter getter = newExternalStorageGetter(
					cacheConfig(new HashSet<>(), Collections.singletonList("POLICY_ID")));

			AtomicInteger constructed = new AtomicInteger();
			CountDownLatch firstConstructing = new CountDownLatch(1);
			CountDownLatch secondConstructing = new CountDownLatch(1);
			// 构造 mock 只对当前线程生效，每个工作线程各自开启拦截
			Callable<IDataSourceRowsGetter> lookup = () -> {
				try (MockedConstruction<PdkDataSourceRowsGetter> ignored = mockConstruction(PdkDataSourceRowsGetter.class, (mock, context) -> {
					if (constructed.incrementAndGet() == 1) {
						firstConstructing.countDown();
						// 拉长第一次构造，给第二个线程进入的机会
						secondConstructing.await(1, TimeUnit.SECONDS);
					} else {
						secondConstructing.countDown();
					}
				})) {
					return getter.getDataSourceRowsGetter();
				}
			};

			ExecutorService workers = Executors.newFixedThreadPool(2);
			try {
				Future<IDataSourceRowsGetter> first = workers.submit(lookup);
				assertTrue(firstConstructing.await(2, TimeUnit.SECONDS));
				Future<IDataSourceRowsGetter> second = workers.submit(lookup);

				assertSame(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
				assertEquals(1, constructed.get(), "并发未命中只能创建一个回源连接器");
			} finally {
				workers.shutdownNow();
			}
		}
	}
}
