package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.RunRawCommandFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.TapTableUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class QueryDataBaseDataServiceTest {

	@Nested
	class QueryV2Test {

		private MockedStatic<TapTableUtil> tapTableUtilMock;
		private MockedStatic<BeanUtil> beanUtilMock;
		private MockedStatic<HazelcastTaskService> taskServiceMock;
		private MockedStatic<ConnectionUtil> connectionUtilMock;
		private MockedStatic<PDKInvocationMonitor> invocationMonitorMock;
		private MockedStatic<PDKIntegration> pdkIntegrationMock;
		private QueryDataBaseDataService spyService;
		private ConnectorFunctions connectorFunctions;
		private RunRawCommandFunction runRawCommandFunction;

		@BeforeEach
		void setUp() {
			tapTableUtilMock = Mockito.mockStatic(TapTableUtil.class);
			beanUtilMock = Mockito.mockStatic(BeanUtil.class);
			taskServiceMock = Mockito.mockStatic(HazelcastTaskService.class);
			connectionUtilMock = Mockito.mockStatic(ConnectionUtil.class);
			invocationMonitorMock = Mockito.mockStatic(PDKInvocationMonitor.class);
			pdkIntegrationMock = Mockito.mockStatic(PDKIntegration.class);

			beanUtilMock.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(mock(HttpClientMongoOperator.class));
			HazelcastTaskService taskService = mock(HazelcastTaskService.class);
			Connections connections = new Connections();
			connections.setPdkHash("hash");
			when(taskService.getConnection("conn")).thenReturn(connections);
			taskServiceMock.when(HazelcastTaskService::taskService).thenReturn(taskService);
			connectionUtilMock.when(() -> ConnectionUtil.getDatabaseType(any(), anyString()))
					.thenReturn(mock(DatabaseTypeEnum.DatabaseType.class));

			ConnectorNode connectorNode = mock(ConnectorNode.class);
			connectorFunctions = mock(ConnectorFunctions.class);
			runRawCommandFunction = mock(RunRawCommandFunction.class);
			when(connectorFunctions.getRunRawCommandFunction()).thenReturn(runRawCommandFunction);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(connectorNode.getConnectorContext()).thenReturn(mock(TapConnectorContext.class));
			when(connectorNode.getCodecsFilterManager()).thenReturn(mock(TapCodecsFilterManager.class));

			spyService = spy(new QueryDataBaseDataService());
			doReturn(connectorNode).when(spyService).createConnectorNode(anyString(), any(), any(), any());
		}

		@AfterEach
		void tearDown() {
			tapTableUtilMock.close();
			beanUtilMock.close();
			taskServiceMock.close();
			connectionUtilMock.close();
			invocationMonitorMock.close();
			pdkIntegrationMock.close();
		}

		@SuppressWarnings("unchecked")
		private void emit(List<TapEvent> events) throws Throwable {
			doAnswer(invocation -> {
				Consumer<List<TapEvent>> consumer = invocation.getArgument(4);
				consumer.accept(events);
				return null;
			}).when(runRawCommandFunction).run(any(), anyString(), any(), anyInt(), any());
		}

		private List<TapEvent> insertEvents(int count) {
			List<TapEvent> events = new ArrayList<>();
			for (int i = 0; i < count; i++) {
				events.add(TapSimplify.insertRecordEvent(new LinkedHashMap<>(Map.of("id", i)), "orders"));
			}
			return events;
		}

		@Test
		void testMockDataTruncatedToLimitAndSkipsNonInsertEvents() throws Throwable {
			tapTableUtilMock.when(() -> TapTableUtil.getTapTableByConnectionId("conn", "orders")).thenReturn(new TapTable("orders"));
			List<TapEvent> events = new ArrayList<>();
			events.add(new TapDeleteRecordEvent());
			events.addAll(insertEvents(5));
			emit(events);

			List<Map<String, Object>> result = spyService.queryV2("conn", "orders", "{\"a\":1}", true, 3);

			assertEquals(List.of(Map.of("id", 0), Map.of("id", 1), Map.of("id", 2)), result);
			verify(runRawCommandFunction).run(any(), eq("{\"a\":1}"), any(), eq(3), any());
			pdkIntegrationMock.verify(() -> PDKIntegration.releaseAssociateId(anyString()));
		}

		@Test
		void testNotMockDataIsNotTruncated() throws Throwable {
			emit(insertEvents(5));

			List<Map<String, Object>> result = spyService.queryV2("conn", null, "select * from t", false, 3);

			assertEquals(5, result.size());
		}

		@Test
		void testMissingSchemaFallsBackToTableName() throws Throwable {
			tapTableUtilMock.when(() -> TapTableUtil.getTapTableByConnectionId("conn", "orders")).thenReturn(null);
			emit(insertEvents(1));

			spyService.queryV2("conn", "orders", "{}", true, 10);

			ArgumentCaptor<TapTable> tableCaptor = ArgumentCaptor.forClass(TapTable.class);
			verify(runRawCommandFunction).run(any(), anyString(), tableCaptor.capture(), anyInt(), any());
			assertEquals("orders", tableCaptor.getValue().getId());
		}

		@Test
		void testUnsupportedRawCommandThrows() {
			when(connectorFunctions.getRunRawCommandFunction()).thenReturn(null);

			RuntimeException e = assertThrows(RuntimeException.class, () -> spyService.queryV2("conn", null, "select 1", true, 10));
			assertInstanceOf(IllegalStateException.class, e.getCause());
			pdkIntegrationMock.verify(() -> PDKIntegration.releaseAssociateId(anyString()));
		}

		@Test
		void testConnectorErrorIsPropagated() throws Throwable {
			doAnswer(invocation -> {
				throw new IllegalArgumentException("MongoDB raw command command must be a JSON object");
			}).when(runRawCommandFunction).run(any(), anyString(), any(), anyInt(), any());

			RuntimeException e = assertThrows(RuntimeException.class, () -> spyService.queryV2("conn", null, "not json", true, 10));
			assertTrue(e.getMessage().contains("must be a JSON object"));
			pdkIntegrationMock.verify(() -> PDKIntegration.releaseAssociateId(anyString()));
		}

		@Test
		void testErrorIsNotWrapped() throws Throwable {
			doAnswer(invocation -> {
				throw new OutOfMemoryError("oom");
			}).when(runRawCommandFunction).run(any(), anyString(), any(), anyInt(), any());

			assertThrows(OutOfMemoryError.class, () -> spyService.queryV2("conn", null, "select 1", true, 10));
			pdkIntegrationMock.verify(() -> PDKIntegration.releaseAssociateId(anyString()));
		}
	}
}
