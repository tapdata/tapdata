package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTaskTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.DateProcessorNode;
import io.tapdata.MockTaskUtil;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.error.TaskDateProcessorExCode_17;
import io.tapdata.exception.TapCodeException;
import io.tapdata.schema.TapTableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-05-18 17:45
 **/
@DisplayName("Class HazelcastDateProcessorNode Test")
class HazelcastDateProcessorNodeTest extends BaseTaskTest {
	private static final String TAG = HazelcastDateProcessorNodeTest.class.getSimpleName();
	private HazelcastDateProcessorNode hazelcastDateProcessorNode;

	@Nested
	@DisplayName("Method tryProcess test")
	class tryProcessTest {

		private TapdataEvent tapdataEvent;
		private long timestamp;
		private TapUpdateRecordEvent tapUpdateRecordEvent;
		private DateProcessorNode dateProcessorNode;
		private TapTableMap tapTableMap;

		@BeforeEach
		void setUp() {
			taskDto = MockTaskUtil.setUpTaskDtoByJsonFile(String.join(File.separator, TAG, "tryProcessTest1.json"));
			dateProcessorNode = (DateProcessorNode) taskDto.getDag().getNodes().stream().filter(node -> node instanceof DateProcessorNode).findFirst().orElse(null);
			setupContext(dateProcessorNode);
			hazelcastDateProcessorNode = spy(new HazelcastDateProcessorNode(processorBaseContext));
			setBaseProperty(hazelcastDateProcessorNode);
			Map<String, Object> after = new HashMap<>();
			timestamp = 1621339200000L;
			Instant instant = Instant.ofEpochMilli(timestamp);
			after.put("_datetime", new DateTime(instant));
			after.put("_tapDatetimeValue", new TapDateTimeValue(new DateTime(instant)));
			after.put("map", new HashMap<String, Object>() {
				{
					put("_datetime", new DateTime(instant));
					put("_tapDatetimeValue", new TapDateTimeValue(new DateTime(instant)));
				}
			});
			after.put("list", new ArrayList<Object>() {
				{
					add(new HashMap<String, Object>() {
						{
							put("_datetime", new DateTime(instant));
							put("_tapDatetimeValue", new TapDateTimeValue(new DateTime(instant)));
						}
					});
				}
			});
			Map<String, Object> before = new HashMap<>();
			before.put("_datetime", new DateTime(instant));
			before.put("_tapDatetimeValue", new TapDateTimeValue(new DateTime(instant)));
			before.put("map", new HashMap<String, Object>() {
				{
					put("_datetime", new DateTime(instant));
					put("_tapDatetimeValue", new TapDateTimeValue(new DateTime(instant)));
				}
			});
			before.put("list", new ArrayList<Object>() {
				{
					add(new HashMap<String, Object>() {
						{
							put("_datetime", new DateTime(instant));
							put("_tapDatetimeValue", new TapDateTimeValue(new DateTime(instant)));
						}
					});
				}
			});
			tapUpdateRecordEvent = TapUpdateRecordEvent.create().init();
			tapUpdateRecordEvent.setBefore(before);
			tapUpdateRecordEvent.setAfter(after);
			tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			TapTable tapTable = new TapTable("91936e19-01b2-47ce-a5c3-73b66131c437");
			tapTable.add(new TapField("_datetime", "now"));
			tapTable.add(new TapField("_tapDatetimeValue", "now"));
			tapTable.add(new TapField("map._datetime", "now"));
			tapTable.add(new TapField("map._tapDatetimeValue", "now"));
			tapTable.add(new TapField("list._datetime", "now"));
			tapTable.add(new TapField("list._tapDatetimeValue", "now"));
			tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get(tapTable.getId())).thenReturn(tapTable);
			when(processorBaseContext.getTapTableMap()).thenReturn(tapTableMap);
		}

		@Test
		@DisplayName("test add 8 hours")
		void test1() {
			long expectTimestamp = timestamp + TimeUnit.HOURS.toMillis(8L);
			hazelcastDateProcessorNode.tryProcess(tapdataEvent, (event, processResult) -> assertTimestamp(event, expectTimestamp));
		}

		@Test
		@DisplayName("test minus 8 hours")
		void test2() {
			long expectTimestamp = timestamp - TimeUnit.HOURS.toMillis(8L);
			dateProcessorNode.setAdd(false);
			setupContext(dateProcessorNode);
			hazelcastDateProcessorNode = spy(new HazelcastDateProcessorNode(processorBaseContext));
			setBaseProperty(hazelcastDateProcessorNode);
			when(processorBaseContext.getTapTableMap()).thenReturn(tapTableMap);
			hazelcastDateProcessorNode.tryProcess(tapdataEvent, (event, processResult) -> assertTimestamp(event, expectTimestamp));
		}

		private void assertTimestamp(TapdataEvent event, long expectTimestamp) {
			AllLayerMapIterator allLayerMapIterator = new AllLayerMapIterator();
			Map<String, Object> before = ((TapUpdateRecordEvent) event.getTapEvent()).getBefore();
			allLayerMapIterator.iterate(before, (key, value, recursive) -> {
				if (value instanceof DateTime) {
					assertEquals(expectTimestamp, ((DateTime) value).toInstant().toEpochMilli());
				} else if (value instanceof TapDateTimeValue) {
					assertEquals(expectTimestamp, ((TapDateTimeValue) value).getValue().toInstant().toEpochMilli());
				}
				return null;
			});
			Map<String, Object> after = ((TapUpdateRecordEvent) event.getTapEvent()).getAfter();
			allLayerMapIterator.iterate(after, (key, value, recursive) -> {
				if (value instanceof DateTime) {
					assertEquals(expectTimestamp, ((DateTime) value).toInstant().toEpochMilli());
				} else if (value instanceof TapDateTimeValue) {
					assertEquals(expectTimestamp, ((TapDateTimeValue) value).getValue().toInstant().toEpochMilli());
				}
				return null;
			});
		}

		@Test
		@DisplayName("test wrong type")
		void test3() {
			tapUpdateRecordEvent.getBefore().put("_datetime", "2024-05-18 17:45:00");
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastDateProcessorNode.tryProcess(tapdataEvent, (event, processResult) -> {
			}));
			assertEquals(TaskDateProcessorExCode_17.SELECTED_TYPE_IS_NON_TIME, tapCodeException.getCode());
		}

		@Test
		@DisplayName("test not found tapTable")
		void test4() {
			when(tapTableMap.get(anyString())).thenReturn(null);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastDateProcessorNode.tryProcess(tapdataEvent, (event, processResult) -> {
			}));
			assertEquals(TaskDateProcessorExCode_17.INIT_TARGET_TABLE_TAP_TABLE_NULL, tapCodeException.getCode());
		}

		@Test
		@DisplayName("test value is null")
		void test5() {
			tapUpdateRecordEvent.getBefore().put("_datetime", null);
			assertDoesNotThrow(() -> hazelcastDateProcessorNode.tryProcess(tapdataEvent, (event, processResult) -> {
			}));
			assertNull(tapUpdateRecordEvent.getBefore().get("_datetime"));
		}
	}
}