package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTaskTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import io.tapdata.MockTaskUtil;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author samuel
 * @Description
 * @create 2024-07-24 16:28
 **/
class HazelcastMigrateFieldRenameProcessorNodeTest extends BaseTaskTest {
	private static final String TAG = HazelcastMigrateFieldRenameProcessorNodeTest.class.getSimpleName();
	private MigrateFieldRenameProcessorNode migrateFieldRenameProcessorNode;
	private HazelcastMigrateFieldRenameProcessorNode hazelcastMigrateFieldRenameProcessorNode;

	@Nested
	@DisplayName("Method tryProcess test")
	class tryProcessTest {
		@Test
		@DisplayName("test operate all fields")
		void test1() {
			taskDto = MockTaskUtil.setUpTaskDtoByJsonFile(String.join(File.separator, TAG, "tryProcessTest1.json"));
			migrateFieldRenameProcessorNode = (MigrateFieldRenameProcessorNode) taskDto.getDag().getNodes().stream().filter(n -> n instanceof MigrateFieldRenameProcessorNode).findFirst().orElse(null);
			setupContext(migrateFieldRenameProcessorNode);
			hazelcastMigrateFieldRenameProcessorNode = new HazelcastMigrateFieldRenameProcessorNode(processorBaseContext);
			List<Map<String, Object>> data = json2Pojo(String.join(File.separator, "task", "json", TAG, "tryProcessTest1_data.json"), new TypeReference<List<Map<String, Object>>>() {
			});
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().init().table("dummy_test");
			tapInsertRecordEvent.setAfter(data.get(0));
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);

			hazelcastMigrateFieldRenameProcessorNode.tryProcess(tapdataEvent, (event, processResult) -> assertEquals(data.get(1), ((TapInsertRecordEvent) event.getTapEvent()).getAfter()));
		}

		@Test
		@DisplayName("test operate specified field")
		void test2() {
			taskDto = MockTaskUtil.setUpTaskDtoByJsonFile(String.join(File.separator, TAG, "tryProcessTest2.json"));
			migrateFieldRenameProcessorNode = (MigrateFieldRenameProcessorNode) taskDto.getDag().getNodes().stream().filter(n -> n instanceof MigrateFieldRenameProcessorNode).findFirst().orElse(null);
			setupContext(migrateFieldRenameProcessorNode);
			hazelcastMigrateFieldRenameProcessorNode = new HazelcastMigrateFieldRenameProcessorNode(processorBaseContext);
			List<Map<String, Object>> data = json2Pojo(String.join(File.separator, "task", "json", TAG, "tryProcessTest2_data.json"), new TypeReference<List<Map<String, Object>>>() {
			});
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().init().table("dummy_test");
			tapInsertRecordEvent.setAfter(data.get(0));
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);

			hazelcastMigrateFieldRenameProcessorNode.tryProcess(tapdataEvent, (event, processResult) -> assertEquals(data.get(1), ((TapInsertRecordEvent) event.getTapEvent()).getAfter()));
		}
	}
}