package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-01-22 18:46
 **/
@DisplayName("HazelcastTargetPdkBaseNode Class Test")
public class HazelcastTargetPdkBaseNodeTest extends BaseHazelcastNodeTest {
	private HazelcastTargetPdkBaseNode hazelcastTargetPdkBaseNode;

	@BeforeEach
	void setUp() {
		hazelcastTargetPdkBaseNode = mock(HazelcastTargetPdkBaseNode.class);
		when(hazelcastTargetPdkBaseNode.getDataProcessorContext()).thenReturn(dataProcessorContext);
	}

	@Nested
	@DisplayName("fromTapValueMergeInfo method test")
	class fromTapValueMergeInfoTest {
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).fromTapValueMergeInfo(any());
		}

		@Test
		@DisplayName("main process test")
		void testMainProcess() {
			MergeInfo mergeInfo = new MergeInfo();
			TapdataEvent tapdataEvent = new TapdataEvent();
			TapEvent tapEvent = mock(TapEvent.class);
			when(tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY)).thenReturn(mergeInfo);
			tapdataEvent.setTapEvent(tapEvent);

			hazelcastTargetPdkBaseNode.fromTapValueMergeInfo(tapdataEvent);
			verify(hazelcastTargetPdkBaseNode, times(1)).recursiveMergeInfoTransformFromTapValue(any());
		}

		@Test
		@DisplayName("when tapEvent not have mergeInfo")
		void notHaveMergeInfo() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			TapEvent tapEvent = mock(TapEvent.class);
			when(tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY)).thenReturn(null);
			tapdataEvent.setTapEvent(tapEvent);

			hazelcastTargetPdkBaseNode.fromTapValueMergeInfo(tapdataEvent);
			verify(hazelcastTargetPdkBaseNode, times(0)).recursiveMergeInfoTransformFromTapValue(any());
		}
	}

	@Nested
	@DisplayName("recursiveMergeInfoTransformFromTapValue method test")
	class recursiveMergeInfoTransformFromTapValueTest {
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).recursiveMergeInfoTransformFromTapValue(any());
		}

		@Test
		@DisplayName("main process test")
		void testMainProcess() {
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
			MergeLookupResult mergeLookupResult = new MergeLookupResult();
			Map<String, Object> data = new HashMap<>();
			data.put("_id", 1);
			mergeLookupResult.setData(data);
			mergeLookupResults.add(mergeLookupResult);
			TapTable tapTable = new TapTable();
			mergeLookupResult.setTapTable(tapTable);
			List<MergeLookupResult> childMergeLookupResults = new ArrayList<>();
			MergeLookupResult childMergeLookupResult = new MergeLookupResult();
			Map<String, Object> childData = new HashMap<>(data);
			childMergeLookupResult.setData(childData);
			childMergeLookupResults.add(childMergeLookupResult);
			childMergeLookupResult.setTapTable(tapTable);
			TapCodecsFilterManager tapCodecsFilterManager = mock(TapCodecsFilterManager.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "codecsFilterManager", tapCodecsFilterManager);
			mergeLookupResult.setMergeLookupResults(childMergeLookupResults);

			hazelcastTargetPdkBaseNode.recursiveMergeInfoTransformFromTapValue(mergeLookupResults);
			verify(hazelcastTargetPdkBaseNode, times(2)).fromTapValue(data, tapCodecsFilterManager, tapTable);
		}

		@Test
		@DisplayName("when mergeLookupResults is empty")
		void mergeLookupResultsIsEmpty() {
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();

			hazelcastTargetPdkBaseNode.recursiveMergeInfoTransformFromTapValue(mergeLookupResults);
			verify(hazelcastTargetPdkBaseNode, times(0)).fromTapValue(any(Map.class), any(TapCodecsFilterManager.class), any(TapTable.class));
		}
	}
	@DisplayName("test ignorePksAndIndices normal")
	@Test
	void ignorePksAndIndicesTest1(){
		TapTable tapTable = new TapTable();
		TapField field = getField("_id");
		TapField index = getField("index");
		tapTable.add(field);
		tapTable.add(index);
		List<String> list= Arrays.asList("_id","index");

		HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable,list);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		TapField idField = nameFieldMap.get("_id");
		TapField indexField = nameFieldMap.get("index");
		assertEquals(2,nameFieldMap.size());
		assertEquals(0,idField.getPrimaryKeyPos());
		assertEquals(0,indexField.getPrimaryKeyPos());
		assertEquals(false,indexField.getPrimaryKey());
		assertEquals(false,idField.getPrimaryKey());
	}
	@DisplayName("test ignorePksAndIndices logic primary key is null")
	@Test
	void ignorePksAndIndicesTest2(){
		TapTable tapTable = new TapTable();
		TapField field = getField("_id");
		TapField index = getField("index");
		tapTable.add(field);
		tapTable.add(index);
		HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable,null);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		TapField idField = nameFieldMap.get("_id");
		TapField indexField = nameFieldMap.get("index");
		assertEquals(2,nameFieldMap.size());
		assertEquals(0,idField.getPrimaryKeyPos());
		assertEquals(0,indexField.getPrimaryKeyPos());
		assertEquals(false,indexField.getPrimaryKey());
		assertEquals(false,idField.getPrimaryKey());
	}
	public TapField getField(String name) {
		TapField field = new TapField();
		field.setName(name);
		return field;
	}

	@Test
	void testSplitDDL2NewBatch() {
		BiFunction<String, String, TapdataEvent> generateEvent = (id, type) -> {
			TapdataEvent tapdataEvent = new TapdataEvent();
			switch (type) {
				case "i":
					tapdataEvent.setTapEvent(TapInsertRecordEvent.create().after(new HashMap<String, Object>() {{
						put("id", id);
					}}));
					break;
				case "u":
					tapdataEvent.setTapEvent(TapUpdateRecordEvent.create().after(new HashMap<String, Object>() {{
						put("id", id);
					}}));
					break;
				case "d":
					tapdataEvent.setTapEvent(TapDeleteRecordEvent.create().before(new HashMap<String, Object>() {{
						put("id", id);
					}}));
					break;
				default:
					tapdataEvent.setTapEvent(new TapNewFieldEvent().field(new TapField(id, "String")));
					break;
			}
			return tapdataEvent;
		};
		List<TapdataEvent> cdcEvents = new ArrayList<>();
		cdcEvents.add(generateEvent.apply("1", "i"));
		cdcEvents.add(generateEvent.apply("2", "i"));
		cdcEvents.add(generateEvent.apply("3", "ddl"));
		cdcEvents.add(generateEvent.apply("4", "i"));
		cdcEvents.add(generateEvent.apply("5", "i"));
		cdcEvents.add(generateEvent.apply("6", "ddl"));
		cdcEvents.add(generateEvent.apply("7", "ddl"));
		cdcEvents.add(generateEvent.apply("8", "i"));

		Consumer<List<TapdataEvent>> subListConsumer = (events) -> {
			if (events.size() > 1) {
				for (TapdataEvent e : events) {
					assertFalse(e.getTapEvent() instanceof TapDDLEvent);
				}
			}
		};

		// test split ddl to new batch
		HazelcastTargetPdkBaseNode targetPdkBaseNode = mock(HazelcastTargetPdkBaseNode.class, CALLS_REAL_METHODS);
		targetPdkBaseNode.splitDDL2NewBatch(cdcEvents, subListConsumer);

		// test not found ddl
		cdcEvents = new ArrayList<>();
		cdcEvents.add(generateEvent.apply("1", "i"));
		cdcEvents.add(generateEvent.apply("2", "i"));
		targetPdkBaseNode.splitDDL2NewBatch(cdcEvents, subListConsumer);

		// test end ddl
		cdcEvents = new ArrayList<>();
		cdcEvents.add(generateEvent.apply("1", "i"));
		cdcEvents.add(generateEvent.apply("2", "ddl"));
		targetPdkBaseNode.splitDDL2NewBatch(cdcEvents, subListConsumer);
	}
}
