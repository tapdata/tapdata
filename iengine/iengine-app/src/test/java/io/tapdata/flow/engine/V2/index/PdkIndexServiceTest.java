package io.tapdata.flow.engine.V2.index;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryIndexesFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * P1-1 · 引擎可复用索引读写通道（{@link PdkIndexService}）。
 *
 * <p>身份/去重策略刻意不在本层（见 ADR-0005）：本服务只做<b>忠实</b>读回（返回连接器给出的
 * 全部物理索引）与<b>忠实</b>创建（原样下发给定索引）。它<b>绝不</b>复用
 * {@code HazelcastTargetPdkDataNode#tapIndexEquals}（按名短路）——按有序字段+方向的身份比对
 * 属于 P3 落地层。</p>
 */
class PdkIndexServiceTest {

	private PdkIndexService service;
	private ConnectorNode connectorNode;
	private ConnectorFunctions connectorFunctions;
	private TapConnectorContext connectorContext;
	private TapTable table;

	@BeforeEach
	void setUp() {
		service = new PdkIndexService();
		connectorNode = mock(ConnectorNode.class);
		connectorFunctions = mock(ConnectorFunctions.class);
		connectorContext = mock(TapConnectorContext.class);
		table = new TapTable("orders");
		when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
		when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
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
	@DisplayName("queryIndexes 忠实读回连接器给出的全部物理索引，不按名/身份过滤")
	void queryIndexes_returnsAllPhysicalIndexes() throws Throwable {
		QueryIndexesFunction fn = mock(QueryIndexesFunction.class);
		when(connectorFunctions.getQueryIndexesFunction()).thenReturn(fn);
		TapIndex a = index("idx_a", "a", true);
		TapIndex b = index("idx_b", "b", false);
		doAnswer(inv -> {
			Consumer<List<TapIndex>> consumer = inv.getArgument(2);
			consumer.accept(Arrays.asList(a, b));
			return null;
		}).when(fn).query(eq(connectorContext), eq(table), any());

		List<TapIndex> result = service.queryIndexes(connectorNode, table);

		assertEquals(2, result.size());
		assertTrue(result.contains(a));
		assertTrue(result.contains(b));
	}

	@Test
	@DisplayName("queryIndexes 跨多次 consumer 回调累积，不覆盖")
	void queryIndexes_accumulatesAcrossConsumerBatches() throws Throwable {
		QueryIndexesFunction fn = mock(QueryIndexesFunction.class);
		when(connectorFunctions.getQueryIndexesFunction()).thenReturn(fn);
		TapIndex a = index("idx_a", "a", true);
		TapIndex b = index("idx_b", "b", true);
		doAnswer(inv -> {
			Consumer<List<TapIndex>> consumer = inv.getArgument(2);
			consumer.accept(Collections.singletonList(a));
			consumer.accept(Collections.singletonList(b));
			return null;
		}).when(fn).query(any(), any(), any());

		List<TapIndex> result = service.queryIndexes(connectorNode, table);

		assertEquals(2, result.size());
	}

	@Test
	@DisplayName("queryIndexes 连接器不支持查询时返回空列表、不抛异常")
	void queryIndexes_whenUnsupported_returnsEmpty() throws Throwable {
		when(connectorFunctions.getQueryIndexesFunction()).thenReturn(null);

		List<TapIndex> result = service.queryIndexes(connectorNode, table);

		assertNotNull(result);
		assertTrue(result.isEmpty());
	}

	@Test
	@DisplayName("createIndex 委派给连接器 CreateIndexFunction，事件原样透传")
	void createIndex_delegatesToConnectorFunction() throws Throwable {
		CreateIndexFunction fn = mock(CreateIndexFunction.class);
		when(connectorFunctions.getCreateIndexFunction()).thenReturn(fn);
		TapCreateIndexEvent event = new TapCreateIndexEvent();

		try (MockedStatic<PDKInvocationMonitor> monitor = mockStatic(PDKInvocationMonitor.class)) {
			monitor.when(() -> PDKInvocationMonitor.invoke(any(), any(PDKMethod.class), any(CommonUtils.AnyError.class), anyString()))
					.thenAnswer(inv -> {
						((CommonUtils.AnyError) inv.getArgument(2)).run();
						return null;
					});

			service.createIndex(connectorNode, table, event);
		}

		verify(fn, times(1)).createIndex(connectorContext, table, event);
	}

	@Test
	@DisplayName("createIndex 连接器不支持创建时抛异常，不静默跳过")
	void createIndex_whenUnsupported_throws() {
		when(connectorFunctions.getCreateIndexFunction()).thenReturn(null);

		assertThrows(Exception.class, () -> service.createIndex(connectorNode, table, new TapCreateIndexEvent()));
	}
}
