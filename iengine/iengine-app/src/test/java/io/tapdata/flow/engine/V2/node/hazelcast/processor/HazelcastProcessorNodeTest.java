package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTaskTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TransformToTapValueResult;
import com.tapdata.entity.dataflow.Capitalized;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import com.tapdata.tm.commons.dag.process.FieldRenameProcessorNode;
import io.tapdata.MockTaskUtil;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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

	@Nested
	@DisplayName("Method handleTransformToTapValueResult test")
	class handleTransformToTapValueResultTest {
		@BeforeEach
		void setUp() {
			dataProcessorContext = mock(DataProcessorContext.class);
			hazelcastProcessorNode = assertDoesNotThrow(() -> spy(new HazelcastProcessorNode(dataProcessorContext)));
		}

		@Test
		@DisplayName("test all fields rename, only have before transform result")
		void test1() {
			FieldRenameProcessorNode fieldRenameProcessorNode = new FieldRenameProcessorNode();
			ReflectionTestUtils.setField(hazelcastProcessorNode, "fieldsNameTransformMap", new HashMap<>());
			ReflectionTestUtils.setField(hazelcastProcessorNode, "fieldRenameProcessorNode", fieldRenameProcessorNode);
			ReflectionTestUtils.setField(hazelcastProcessorNode, "capitalized", Capitalized.UPPER);
			when(dataProcessorContext.getNode()).thenReturn((Node) fieldRenameProcessorNode);

			TapdataEvent tapdataEvent = new TapdataEvent();
			TransformToTapValueResult transformToTapValueResult = TransformToTapValueResult.create()
					.beforeTransformedToTapValueFieldNames(new HashSet<String>() {{
						add("created");
					}});
			tapdataEvent.setTransformToTapValueResult(transformToTapValueResult);

			hazelcastProcessorNode.handleTransformToTapValueResult(tapdataEvent);

			Set<String> expect = new HashSet<String>() {{
				add("CREATED");
			}};
			assertEquals(expect, tapdataEvent.getTransformToTapValueResult().getBeforeTransformedToTapValueFieldNames());
		}

		@Test
		@DisplayName("test all fields rename, only have after transform result")
		void test2() {
			FieldRenameProcessorNode fieldRenameProcessorNode = new FieldRenameProcessorNode();
			ReflectionTestUtils.setField(hazelcastProcessorNode, "fieldsNameTransformMap", new HashMap<>());
			ReflectionTestUtils.setField(hazelcastProcessorNode, "fieldRenameProcessorNode", fieldRenameProcessorNode);
			ReflectionTestUtils.setField(hazelcastProcessorNode, "capitalized", Capitalized.UPPER);
			when(dataProcessorContext.getNode()).thenReturn((Node) fieldRenameProcessorNode);

			TapdataEvent tapdataEvent = new TapdataEvent();
			TransformToTapValueResult transformToTapValueResult = TransformToTapValueResult.create()
					.afterTransformedToTapValueFieldNames(new HashSet<String>() {{
						add("created");
					}});
			tapdataEvent.setTransformToTapValueResult(transformToTapValueResult);

			hazelcastProcessorNode.handleTransformToTapValueResult(tapdataEvent);

			Set<String> expect = new HashSet<String>() {{
				add("CREATED");
			}};
			assertEquals(expect, tapdataEvent.getTransformToTapValueResult().getAfterTransformedToTapValueFieldNames());
		}

		@Test
		@DisplayName("test create, rename, remove specify fields")
		void test3() {
			FieldProcessorNode.Operation create = new FieldProcessorNode.Operation();
			create.setOp("CREATE");
			create.setField("field_create");
			FieldProcessorNode.Operation rename = new FieldProcessorNode.Operation();
			rename.setOp("RENAME");
			rename.setField("field_old");
			rename.setOperand("field_new");
			FieldProcessorNode.Operation remove = new FieldProcessorNode.Operation();
			remove.setOp("REMOVE");
			remove.setField("field_remove");
			List<FieldProcessorNode.Operation> operations = new ArrayList<>();
			operations.add(create);
			operations.add(rename);
			operations.add(remove);

			FieldProcessorNode fieldProcessorNode = new FieldProcessorNode();
			fieldProcessorNode.setOperations(operations);
			when(dataProcessorContext.getNode()).thenReturn((Node) fieldProcessorNode);

			TapdataEvent tapdataEvent = new TapdataEvent();
			TransformToTapValueResult transformToTapValueResult = TransformToTapValueResult.create()
					.beforeTransformedToTapValueFieldNames(new HashSet<String>() {{
						add("field_old");
						add("field_remove");
					}}).afterTransformedToTapValueFieldNames(new HashSet<String>() {{
						add("field_old");
						add("field_remove");
					}});
			tapdataEvent.setTransformToTapValueResult(transformToTapValueResult);

			hazelcastProcessorNode.handleTransformToTapValueResult(tapdataEvent);

			Set<String> expect = new HashSet<String>() {{
				add("field_create");
				add("field_new");
			}};

			assertEquals(expect, tapdataEvent.getTransformToTapValueResult().getBeforeTransformedToTapValueFieldNames());
			assertEquals(expect, tapdataEvent.getTransformToTapValueResult().getAfterTransformedToTapValueFieldNames());
		}

		@Test
		@DisplayName("test transform result is null or empty")
		void test4() {
			TapdataEvent tapdataEvent = new TapdataEvent();

			hazelcastProcessorNode.handleTransformToTapValueResult(tapdataEvent);
			assertNull(tapdataEvent.getTransformToTapValueResult());

			tapdataEvent.setTransformToTapValueResult(TransformToTapValueResult.create());

			hazelcastProcessorNode.handleTransformToTapValueResult(tapdataEvent);
			assertTrue(tapdataEvent.getTransformToTapValueResult().isEmpty());
		}
	}
}