package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTaskTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import io.tapdata.MockTaskUtil;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author samuel
 * @Description
 * @create 2024-07-24 11:27
 **/
@DisplayName("Class HazelcastProcessorNode Test")
class HazelcastProcessorNodeTest extends BaseTaskTest {
	private static final String TAG = HazelcastProcessorNodeTest.class.getSimpleName();
	private HazelcastProcessorNode hazelcastProcessorNode;

	@BeforeEach
	@SneakyThrows
	void setUp() {
		hazelcastProcessorNode = spy(new HazelcastProcessorNode(dataProcessorContext));
	}

	@Nested
	@DisplayName("Method tryProcess test")
	class tryProcessTest {
		@BeforeEach
		@SneakyThrows
		void setUp() {
			taskDto = MockTaskUtil.setUpTaskDtoByJsonFile(String.join(File.separator, TAG, "tryProcessTest1.json"));
			FieldProcessorNode fieldProcessorNode = (FieldProcessorNode) taskDto.getDag().getNodes().stream().filter(node -> node instanceof FieldProcessorNode).findFirst().orElse(null);
			setupContext(fieldProcessorNode);
			hazelcastProcessorNode = spy(new HazelcastProcessorNode(dataProcessorContext));
			setBaseProperty(hazelcastProcessorNode);
			Processor.Context context = mock(Processor.Context.class);
			hazelcastProcessorNode.doInit(context);
		}

		@Test
		@DisplayName("test rename all fields")
		void test1() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().init();
			List<?> data = json2Pojo(String.join(File.separator, "task", "json", TAG, "tryProcessTest1_data.json"), new TypeReference<List<?>>() {
			});
			tapInsertRecordEvent.setAfter((Map<String, Object>) data.get(0));
			tapdataEvent.setTapEvent(tapInsertRecordEvent);

			hazelcastProcessorNode.tryProcess(tapdataEvent, (event, processResult) -> assertEquals(data.get(1), (((TapInsertRecordEvent) event.getTapEvent()).getAfter())));
		}
	}
}