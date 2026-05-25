package io.tapdata.flow.engine.V2.node.hazelcast.data;

import base.hazelcast.BaseHazelcastNodeTest;
import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.error.VirtualTargetExCode_14;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-22 21:38
 **/
@DisplayName("HazelcastSchemaTargetNode Class Test")
class HazelcastSchemaTargetNodeTest extends BaseHazelcastNodeTest {

	HazelcastSchemaTargetNode hazelcastSchemaTargetNode;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		hazelcastSchemaTargetNode = new HazelcastSchemaTargetNode(dataProcessorContext);
	}

	@Nested
	@DisplayName("DoInit method test")
	class DoInitTest {
		@Test
		@DisplayName("Test init tapTableMap")
		void testDoInitInitTapTableMap() {
			try (MockedStatic<TapTableUtil> tapTableUtilMockedStatic = mockStatic(TapTableUtil.class)) {
				TapTableMap tapTableMap = mock(TapTableMap.class);
				tapTableUtilMockedStatic.when(() -> TapTableUtil.getTapTableMap(anyString(), any(Node.class), any()))
						.thenReturn(tapTableMap);
				Node<?> mockNode = mock(Node.class);
				Node<?> spyNode = spy(dataProcessorContext.getNode());
				List preNodes = new ArrayList<>();
				preNodes.add(mockNode);
				doReturn(preNodes).when(spyNode).predecessors();
				when(dataProcessorContext.getNode()).thenReturn((Node) spyNode);
				assertDoesNotThrow(() -> hazelcastSchemaTargetNode.doInit(jetContext));
				verify(spyNode, new Times(1)).predecessors();
				Object actualObj = ReflectionTestUtils.getField(hazelcastSchemaTargetNode, "oldTapTableMap");
				assertEquals(tapTableMap, actualObj);
			}
		}

		@Test
		@DisplayName("When predecessors more then one")
		void testDoInitPreNodeMoreThanOne() {
			Node<?> mockNode = mock(Node.class);
			Node<?> spyNode = spy(dataProcessorContext.getNode());
			List preNodes = new ArrayList<>();
			preNodes.add(mockNode);
			preNodes.add(mockNode);
			doReturn(preNodes).when(spyNode).predecessors();
			when(dataProcessorContext.getNode()).thenReturn((Node) spyNode);
			IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> hazelcastSchemaTargetNode.doInit(jetContext));
			assertEquals("HazelcastSchemaTargetNode only allows one predecessor node", illegalArgumentException.getMessage());
		}
	}
	@Test
	void testProcess_DECLARE_ERROR() {
		ObsLogger obsLogger = mock(ObsLogger.class);
		boolean multipleTables = true;
		boolean needToDeclare = true;
		Function<Object, Object> declareFunction = mock(Function.class);
		ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "obsLogger", obsLogger);
		ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "multipleTables", multipleTables);
		ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "needToDeclare", needToDeclare);
		ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "declareFunction", declareFunction);
		hazelcastSchemaTargetNode = spy(hazelcastSchemaTargetNode);
		int ordinal = 1;
		Inbox inbox = mock(Inbox.class);
		when(inbox.isEmpty()).thenReturn(false);
		doCallRealMethod().when(inbox).drainTo(anyList(), anyInt());
		TapdataEvent tapdataEvent = mock(TapdataEvent.class);
		when(inbox.poll()).thenReturn(tapdataEvent);
		when(tapdataEvent.getTapEvent()).thenReturn(mock(TapRecordEvent.class));
		when(hazelcastSchemaTargetNode.isRunning()).thenReturn(true);
		when(declareFunction.apply(anyList())).thenThrow(RuntimeException.class);
		TapCodeException exception = assertThrows(TapCodeException.class, () -> hazelcastSchemaTargetNode.process(ordinal, inbox));
		assertEquals(VirtualTargetExCode_14.DECLARE_ERROR, exception.getCode());
	}

	@Nested
	@DisplayName("getNewTapTable method test")
	class GetNewTapTableTest {

		@BeforeEach
		void beforeEach() {
			ObsLogger obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "obsLogger", obsLogger);
		}

		private TapTable invoke(TapRecordEvent event) {
			return ReflectionTestUtils.invokeMethod(hazelcastSchemaTargetNode, "getNewTapTable", event);
		}

		private TapInsertRecordEvent newEvent(String tableId, Map<String, Object> after) {
			TapInsertRecordEvent event = new TapInsertRecordEvent().init();
			event.setTableId(tableId);
			if (after != null) {
				event.setAfter(after);
			}
			return event;
		}

		@SuppressWarnings("unchecked")
		private void mockOldTapTableMap(String tableId, TapTable oldTable) {
			TapTableMap<String, TapTable> oldMap = mock(TapTableMap.class);
			when(oldMap.containsKey(tableId)).thenReturn(true);
			when(oldMap.get(tableId)).thenReturn(oldTable);
			ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "oldTapTableMap", oldMap);
		}

		@Test
		@DisplayName("when after is empty return table with id only")
		void testEmptyAfter() {
			ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "oldTapTableMap", null);
			TapTable result = invoke(newEvent("t1", null));
			assertNotNull(result);
			assertEquals("t1", result.getId());
			assertTrue(result.getNameFieldMap() == null || result.getNameFieldMap().isEmpty());
			assertNull(result.getIndexList());
		}

		@Test
		@DisplayName("when oldTapTableMap is null, scan after fields without index logic")
		void testNoOldTapTableMap() {
			ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "oldTapTableMap", null);
			Map<String, Object> after = new LinkedHashMap<>();
			after.put("a", "x");
			after.put("b", 1);
			TapTable result = invoke(newEvent("t1", after));
			assertEquals("t1", result.getId());
			LinkedHashMap<String, TapField> nameFieldMap = result.getNameFieldMap();
			assertNotNull(nameFieldMap);
			assertEquals(2, nameFieldMap.size());
			assertTrue(nameFieldMap.containsKey("a"));
			assertTrue(nameFieldMap.containsKey("b"));
			assertNull(result.getIndexList());
		}

		@Test
		@DisplayName("orders fields by old field positions, missing positions go last")
		void testOrderByOldFieldPos() {
			TapTable oldTable = new TapTable("t1");
			oldTable.add(new TapField("a", "varchar(20)").pos(2));
			oldTable.add(new TapField("b", "varchar(20)").pos(1));
			mockOldTapTableMap("t1", oldTable);
			Map<String, Object> after = new LinkedHashMap<>();
			after.put("a", "x");
			after.put("b", "y");
			after.put("c", "z");
			TapTable result = invoke(newEvent("t1", after));
			List<String> order = new ArrayList<>(result.getNameFieldMap().keySet());
			assertEquals(List.of("b", "a", "c"), order);
		}

		@Test
		@DisplayName("retain old indexes whose fields all exist in new table")
		void testRetainOldIndex() {
			TapTable oldTable = new TapTable("t1");
			oldTable.add(new TapField("a", "varchar(20)").pos(1));
			TapIndex idx = new TapIndex().name("idx1").indexField(new TapIndexField().name("a"));
			oldTable.add(idx);
			mockOldTapTableMap("t1", oldTable);
			Map<String, Object> after = new LinkedHashMap<>();
			after.put("a", "x");
			TapTable result = invoke(newEvent("t1", after));
			assertNotNull(result.getIndexList());
			assertEquals(1, result.getIndexList().size());
			assertEquals("idx1", result.getIndexList().get(0).getName());
			List<TapIndexField> fields = result.getIndexList().get(0).getIndexFields();
			assertEquals(1, fields.size());
			assertEquals("a", fields.get(0).getName());
		}

		@Test
		@DisplayName("filter index fields to those existing in new table")
		void testFilterIndexFields() {
			TapTable oldTable = new TapTable("t1");
			oldTable.add(new TapField("a", "varchar(20)").pos(1));
			oldTable.add(new TapField("b", "varchar(20)").pos(2));
			oldTable.add(new TapField("c", "varchar(20)").pos(3));
			TapIndex idx = new TapIndex().name("idx1")
					.indexField(new TapIndexField().name("a"))
					.indexField(new TapIndexField().name("b"))
					.indexField(new TapIndexField().name("x"));
			oldTable.add(idx);
			mockOldTapTableMap("t1", oldTable);
			Map<String, Object> after = new LinkedHashMap<>();
			after.put("a", "x");
			after.put("b", "y");
			TapTable result = invoke(newEvent("t1", after));
			assertEquals(1, result.getIndexList().size());
			List<TapIndexField> fields = result.getIndexList().get(0).getIndexFields();
			assertEquals(2, fields.size());
			assertEquals("a", fields.get(0).getName());
			assertEquals("b", fields.get(1).getName());
		}

		@Test
		@DisplayName("skip index when none of its fields exist in new table")
		void testSkipIndex() {
			TapTable oldTable = new TapTable("t1");
			oldTable.add(new TapField("a", "varchar(20)").pos(1));
			TapIndex idx = new TapIndex().name("idx1")
					.indexField(new TapIndexField().name("x"))
					.indexField(new TapIndexField().name("y"));
			oldTable.add(idx);
			mockOldTapTableMap("t1", oldTable);
			Map<String, Object> after = new LinkedHashMap<>();
			after.put("a", "x");
			TapTable result = invoke(newEvent("t1", after));
			assertNull(result.getIndexList());
		}
	}
}
