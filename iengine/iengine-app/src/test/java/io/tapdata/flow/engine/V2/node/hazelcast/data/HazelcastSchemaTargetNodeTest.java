package io.tapdata.flow.engine.V2.node.hazelcast.data;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
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

	@Nested
	@DisplayName("Test preserving sub attributes after model inference on Js nodes")
	class RetainedOldSubFields{
		HazelcastSchemaTargetNode hstn;
		TapTable tapTable;
		LinkedHashMap<String, TapField> oldNameFieldMap;
		TapField f;
		LinkedHashMap<String, TapField> nameFieldMap;
		TapField field;
		Map<String, Object> afterValue;
		@BeforeEach
		void init() {
			hstn = mock(HazelcastSchemaTargetNode.class);

			f = mock(TapField.class);
			field = mock(TapField.class);

			tapTable = new TapTable("id");
			nameFieldMap = new LinkedHashMap<>();
			nameFieldMap.put("f", f);
			when(f.getTapType()).thenReturn(new TapMap());
			tapTable.setNameFieldMap(nameFieldMap);

			oldNameFieldMap = new LinkedHashMap<>();
			oldNameFieldMap.put("f", field);
			when(field.getTapType()).thenReturn(new TapMap());
			afterValue = new HashMap<>();

			doCallRealMethod().when(hstn).retainedOldSubFields(tapTable, oldNameFieldMap, afterValue);
		}
		@Test
		void testNormal() {
			Assertions.assertDoesNotThrow(() -> hstn.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
			Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
			Assertions.assertNotNull(tapTable.getNameFieldMap().get("f"));
		}

		@Test
		void testOldNameFieldMapIsEmpty() {
			oldNameFieldMap.clear();
			Assertions.assertDoesNotThrow(() -> hstn.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
			Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
			Assertions.assertNotNull(tapTable.getNameFieldMap().get("f"));
		}

		@Test
		void testContainsSubFieldButNotContainsSubField() {
			TapField subField = mock(TapField.class);
			when(subField.getName()).thenReturn("f.id");
			oldNameFieldMap.put("f.id", subField);
			afterValue.put("f", new HashMap<>());
			Assertions.assertDoesNotThrow(() -> hstn.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
			Assertions.assertEquals(2, tapTable.getNameFieldMap().size());
			Assertions.assertNotNull(tapTable.getNameFieldMap().get("f.id"));
		}
		@Test
		void testContainsSubFieldAndContainsSubField() {
			TapField subField = mock(TapField.class);
			when(subField.getName()).thenReturn("f.id");
			oldNameFieldMap.put("f.id", subField);
			afterValue.put("f", new HashMap<>());
			afterValue.put("f.id", "id");
			Assertions.assertDoesNotThrow(() -> hstn.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
			Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
			Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
		}

		@Test
		void testNotContainsSubFieldButNotContainsSubField() {
			TapField subField = mock(TapField.class);
			when(subField.getName()).thenReturn("f.id");
			oldNameFieldMap.put("f.id", subField);
			Assertions.assertDoesNotThrow(() -> hstn.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
			Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
			Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
		}
		@Test
		void testOldNameFieldMapNotContainsFatherField() {
			TapField subField = mock(TapField.class);
			when(subField.getName()).thenReturn("f.id");
			oldNameFieldMap.put("f.id", subField);
			oldNameFieldMap.remove("f");
			afterValue.put("f", new HashMap<>());
			Assertions.assertDoesNotThrow(() -> hstn.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
			Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
			Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
		}
		@Test
		void testTapTableNotContainsFatherField() {
			TapField subField = mock(TapField.class);
			when(subField.getName()).thenReturn("f.id");
			nameFieldMap.remove("f");
			oldNameFieldMap.put("f.id", subField);
			afterValue.put("f", new HashMap<>());
			Assertions.assertDoesNotThrow(() -> hstn.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
			Assertions.assertEquals(0, tapTable.getNameFieldMap().size());
			Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
		}
		@Test
		void testTapTableContainsFatherFieldButFieldTapTypeIsNull() {
			TapField subField = mock(TapField.class);
			when(subField.getName()).thenReturn("f.id");
			when(f.getTapType()).thenReturn(null);
			oldNameFieldMap.put("f.id", subField);
			afterValue.put("f", new HashMap<>());
			Assertions.assertDoesNotThrow(() -> hstn.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
			Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
			Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
		}
		@Test
		void testBeforeTapNotContainsAfterTap() {
			TapField subField = mock(TapField.class);
			when(subField.getName()).thenReturn("f.id");
			when(f.getTapType()).thenReturn(new TapArray());
			oldNameFieldMap.put("f.id", subField);
			afterValue.put("f", new HashMap<>());
			Assertions.assertDoesNotThrow(() -> hstn.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
			Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
			Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
		}
	}
}
