package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapYear;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.schema.TapTableMap;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HazelcastSampleSourcePdkDataNodeTest {
	private static final String TABLE = "test";

	@Test
	void testExplicitEventTypeTakesPrecedenceOverInferredType() {
		HazelcastSampleSourcePdkDataNode node = createNode(field("id", new TapNumber()));
		TaskDto taskDto = taskDto("{\"before\":{\"id\":1},\"after\":{\"id\":2}}", "insert");

		List<TapEvent> tapEvents = node.buildTestRunInputTapEvents(taskDto, TABLE);

		assertEquals(1, tapEvents.size());
		assertInstanceOf(TapInsertRecordEvent.class, tapEvents.get(0));
		assertEquals(2, ((TapInsertRecordEvent) tapEvents.get(0)).getAfter().get("id"));
	}

	@Test
	void testInferEventTypesWhenExplicitTypeMissing() {
		HazelcastSampleSourcePdkDataNode node = createNode(field("id", new TapNumber()));
		TaskDto taskDto = taskDto("[{\"after\":{\"id\":1}},{\"before\":{\"id\":2},\"after\":{\"id\":3}},{\"before\":{\"id\":4}}]", null);

		List<TapEvent> tapEvents = node.buildTestRunInputTapEvents(taskDto, TABLE);

		assertEquals(3, tapEvents.size());
		assertInstanceOf(TapInsertRecordEvent.class, tapEvents.get(0));
		assertInstanceOf(TapUpdateRecordEvent.class, tapEvents.get(1));
		assertInstanceOf(TapDeleteRecordEvent.class, tapEvents.get(2));
	}

	@Test
	void testConvertTopLevelValuesBySchemaTypeBeforeToTapValue() {
		HazelcastSampleSourcePdkDataNode node = createNode(
				field("name", new TapString()),
				field("score", new TapNumber()),
				field("enabled", new TapBoolean()),
				field("createdAt", new TapDateTime()),
				field("birthday", new TapDate()),
				field("clockAt", new TapTime()),
				field("yearValue", new TapYear()),
				field("tags", new TapArray()),
				field("profile", new TapMap())
		);
		TaskDto taskDto = taskDto("{\"after\":{\"name\":123,\"score\":\"12.5\",\"enabled\":\"0\",\"createdAt\":\"2024-01-02 03:04:05\",\"birthday\":\"2024-01-02\",\"clockAt\":\"03:04:05\",\"yearValue\":\"2024\",\"tags\":\"[1,2,3]\",\"profile\":\"{\\\"a\\\":1}\"}}", "insert");

		List<TapEvent> tapEvents = node.buildTestRunInputTapEvents(taskDto, TABLE);
		Map<String, Object> after = ((TapInsertRecordEvent) tapEvents.get(0)).getAfter();

		assertEquals("123", after.get("name"));
		assertEquals(12.5D, after.get("score"));
		assertEquals(false, after.get("enabled"));
		assertInstanceOf(TapDateTimeValue.class, after.get("createdAt"));
		assertInstanceOf(TapDateValue.class, after.get("birthday"));
		assertInstanceOf(TapTimeValue.class, after.get("clockAt"));
		assertInstanceOf(TapYearValue.class, after.get("yearValue"));
		assertTapValue(TapArrayValue.class, List.of(1, 2, 3), after.get("tags"));
		assertTapValue(TapMapValue.class, Map.of("a", 1), after.get("profile"));
	}

	@Test
	void testConvertNestedMapAndListValuesByDottedFieldPath() {
		HazelcastSampleSourcePdkDataNode node = createNode(
				field("profile", new TapMap()),
				field("profile.name", new TapString()),
				field("profile.age", new TapNumber()),
				field("items", new TapArray()),
				field("items.code", new TapString()),
				field("items.enabled", new TapBoolean()),
				field("items.score", new TapNumber())
		);
		TaskDto taskDto = taskDto("{\"after\":{\"profile\":{\"name\":123,\"age\":\"18\"},\"items\":[{\"code\":456,\"enabled\":\"0\",\"score\":\"12.5\"},{\"code\":789,\"enabled\":1,\"score\":7}]}}", "insert");

		List<TapEvent> tapEvents = node.buildTestRunInputTapEvents(taskDto, TABLE);
		Map<String, Object> after = ((TapInsertRecordEvent) tapEvents.get(0)).getAfter();

		assertInstanceOf(TapMapValue.class, after.get("profile"));
		Map<String, Object> profile = (Map<String, Object>) ((TapMapValue) after.get("profile")).getValue();
		assertEquals("123", profile.get("name"));
		assertInstanceOf(Number.class, profile.get("age"));
		assertEquals(18D, ((Number) profile.get("age")).doubleValue());

		assertInstanceOf(TapArrayValue.class, after.get("items"));
		List<?> items = (List<?>) ((TapArrayValue) after.get("items")).getValue();
		Map<String, Object> firstItem = unwrapMapValue(items.get(0));
		Map<String, Object> secondItem = unwrapMapValue(items.get(1));
		assertEquals("456", firstItem.get("code"));
		assertEquals(false, firstItem.get("enabled"));
		assertEquals(12.5D, firstItem.get("score"));
		assertEquals("789", secondItem.get("code"));
		assertEquals(true, secondItem.get("enabled"));
		assertEquals(7, secondItem.get("score"));
	}


	@Test
	void testConversionFailureKeepsOriginalValueSemantics() {
		HazelcastSampleSourcePdkDataNode node = createNode(field("score", new TapNumber()));
		TaskDto taskDto = taskDto("{\"after\":{\"score\":\"not-a-number\"}}", "insert");

		List<TapEvent> tapEvents = node.buildTestRunInputTapEvents(taskDto, TABLE);
		Map<String, Object> after = ((TapInsertRecordEvent) tapEvents.get(0)).getAfter();

		assertEquals("not-a-number", after.get("score"));
	}

	@Test
	void testRejectInvalidJson() {
		HazelcastSampleSourcePdkDataNode node = createNode(field("id", new TapNumber()));
		TaskDto taskDto = taskDto("{invalid}", null);

		assertThrows(CoreException.class, () -> node.buildTestRunInputTapEvents(taskDto, TABLE));
	}

	@Test
	void testRejectMissingBeforeOrAfterWhenTypeCannotBeInferred() {
		HazelcastSampleSourcePdkDataNode node = createNode(field("id", new TapNumber()));
		TaskDto taskDto = taskDto("{\"id\":1}", null);

		assertThrows(CoreException.class, () -> node.buildTestRunInputTapEvents(taskDto, TABLE));
	}

	@Test
	void testReturnsNullWhenJsonMissing() {
		HazelcastSampleSourcePdkDataNode node = createNode(field("id", new TapNumber()));

		assertNull(node.buildTestRunInputTapEvents(new TaskDto(), TABLE));
	}

	private HazelcastSampleSourcePdkDataNode createNode(TapField... fields) {
		DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
		TaskDto taskDto = new TaskDto();
		taskDto.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
		when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
		TapTable tapTable = new TapTable(TABLE);
		for (TapField field : fields) {
			tapTable.add(field);
		}
		TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("node-1", List.of(tapTable), null);
		when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);

		HazelcastSampleSourcePdkDataNode node = new HazelcastSampleSourcePdkDataNode(dataProcessorContext);
		ReflectionTestUtils.setField(node, "codecsFilterManager", TapCodecsFilterManager.create(TapCodecsRegistry.create()));
		return node;
	}

	private TapField field(String name, io.tapdata.entity.schema.type.TapType tapType) {
		return new TapField(name, null).tapType(tapType);
	}

	private TaskDto taskDto(String json, String eventType) {
		TaskDto taskDto = new TaskDto();
		taskDto.setTestRunInputEventJson(json);
		taskDto.setTestRunInputEventType(eventType);
		return taskDto;
	}

	private void assertTapValue(Class<? extends TapValue> type, Object expectedValue, Object actual) {
		assertInstanceOf(type, actual);
		assertEquals(expectedValue, ((TapValue<?, ?>) actual).getValue());
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> unwrapMapValue(Object value) {
		if (value instanceof TapMapValue) {
			return (Map<String, Object>) ((TapMapValue) value).getValue();
		}
		return (Map<String, Object>) value;
	}
}
