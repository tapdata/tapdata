package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTaskTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import io.tapdata.MockTaskUtil;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-07-24 16:28
 **/
class HazelcastMigrateFieldRenameProcessorNodeTest extends BaseTaskTest {
	private static final String TAG = HazelcastMigrateFieldRenameProcessorNodeTest.class.getSimpleName();
	private MigrateFieldRenameProcessorNode migrateFieldRenameProcessorNode;
	private HazelcastMigrateFieldRenameProcessorNode hazelcastMigrateFieldRenameProcessorNode;

	@BeforeEach
	void beforeEach() {
		taskDto = MockTaskUtil.setUpTaskDtoByJsonFile(String.join(File.separator, TAG, "tryProcessTest1.json"));
		migrateFieldRenameProcessorNode = (MigrateFieldRenameProcessorNode) taskDto.getDag().getNodes().stream().filter(n -> n instanceof MigrateFieldRenameProcessorNode).findFirst().orElse(null);
		setupContext(migrateFieldRenameProcessorNode);
		hazelcastMigrateFieldRenameProcessorNode = new HazelcastMigrateFieldRenameProcessorNode(processorBaseContext);
	}
	@Nested
	@DisplayName("Method tryProcess test")
	class tryProcessTest {
		@Test
		@DisplayName("test operate all fields")
		void test1() {
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
	@Nested
	class applyFieldInfo{
		private HazelcastMigrateFieldRenameProcessorNode.DataExecutor applyConfig;
		@BeforeEach
		void beforeEach(){
			applyConfig = mock(HazelcastMigrateFieldRenameProcessorNode.DataExecutor.class);
		}
		@Test
		@DisplayName("test when fieldInfoMaps is empty or not contains tableName")
		void test1() {
			doCallRealMethod().when(applyConfig).applyFieldInfo(anyString(), anyMap(), any());
			boolean actual = applyConfig.applyFieldInfo("test", new HashMap<>(), mock(MigrateFieldRenameProcessorNode.IOperator.class));
			assertFalse(actual);
			Map<String, Map<String, FieldInfo>> fieldInfoMaps = new HashMap<>();
			fieldInfoMaps.put("test1", new HashMap<>());
			ReflectionTestUtils.setField(applyConfig, "fieldInfoMaps", fieldInfoMaps);
			actual = applyConfig.applyFieldInfo("test", new HashMap<>(), mock(MigrateFieldRenameProcessorNode.IOperator.class));
			assertFalse(actual);
		}
		@Test
		@DisplayName("test when operatorParam contains targetFieldName")
		void test2() {
			Map<String, Map<String, FieldInfo>> fieldInfoMaps = new HashMap<>();
			HashMap<String, FieldInfo> fieldInfoMap = new HashMap<>();
			FieldInfo fieldA = new FieldInfo();
			fieldA.setTargetFieldName("B");
			fieldInfoMap.put("A", fieldA);
			FieldInfo fieldB = new FieldInfo();
			fieldB.setTargetFieldName("D");
			fieldInfoMap.put("B", fieldB);
			fieldInfoMaps.put("test", fieldInfoMap);
			ReflectionTestUtils.setField(applyConfig, "fieldInfoMaps", fieldInfoMaps);
			Map<String, Map<String, FieldInfo>> fieldInfoTempMaps = new HashMap<>();
			fieldInfoTempMaps.put("test", new HashMap<>());
			ReflectionTestUtils.setField(applyConfig, "fieldInfoTempMaps", fieldInfoTempMaps);

			Map<String, Object> operatorParam = new HashMap<>();
			operatorParam.put("A",1);
			operatorParam.put("B",2);
			operatorParam.put("C",3);

			MigrateFieldRenameProcessorNode.IOperator operator = new MigrateFieldRenameProcessorNode.IOperator<Map<String, Object>>() {
				@Override
				public void renameField(Map<String, Object> param, String fromName, String toName) {
					MapUtil.replaceKey(fromName, param, toName);
				}

				@Override
				public void deleteField(Map<String, Object> param, String originalName) {
				}

				@Override
				public Object renameFieldWithReturn(Map<String, Object> param, String fromName, String toName) {
					return null;
				}
			};
			doCallRealMethod().when(applyConfig).applyFieldInfo("test", operatorParam, operator);
			applyConfig.applyFieldInfo("test", operatorParam, operator);
			assertEquals(1, operatorParam.get("B"));
			assertEquals(2, operatorParam.get("D"));
			assertEquals(3, operatorParam.get("C"));
		}
	}
}