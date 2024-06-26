package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-21 22:26
 **/
@DisplayName("HazelcastJavaScriptProcessorNode Class Test")
class HazelcastJavaScriptProcessorNodeTest extends BaseHazelcastNodeTest {
	private HazelcastJavaScriptProcessorNode hazelcastJavaScriptProcessorNode;
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNodeTest.class);

	@BeforeEach
	protected void beforeEach() {
		super.allSetup();
		hazelcastJavaScriptProcessorNode = new HazelcastJavaScriptProcessorNode(processorBaseContext) {
		};
	}

	@Nested
	@DisplayName("Do close test")
	class DoCloseTest {
		@BeforeEach
		void beforeEach() {
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode, "obsLogger", mockObsLogger);
		}

		@Test
		@DisplayName("ThreadLocal must call remove when do close")
		void testDoCloseThreadLocal() {
			ThreadLocal<Map<String, Object>> processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
			processContextThreadLocal.get().put("test", "test");
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode, "processContextThreadLocal", processContextThreadLocal);
			hazelcastJavaScriptProcessorNode.doClose();
			Object actualObj = ReflectionTestUtils.getField(hazelcastJavaScriptProcessorNode, "processContextThreadLocal");
			assertNotNull(actualObj);
			assertTrue(actualObj instanceof ThreadLocal);
			assertEquals(HashMap.class, ((ThreadLocal<?>) actualObj).get().getClass());
			Map<String, Object> map = ((ThreadLocal<Map<String, Object>>) actualObj).get();
			assertEquals(0, map.size());
		}
	}
	@Nested
	class tryProcessTest{
		private TapdataEvent tapdataEvent;
		private BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer;
		private Invocable engine;
		private TapUpdateRecordEvent tapEvent;
		@BeforeEach
		@SneakyThrows
		void beforeEach(){
			consumer = mock(BiConsumer.class);
			ThreadLocal<Map<String, Object>> processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"processContextThreadLocal",processContextThreadLocal);
			engine = spy(ScriptUtil.getScriptEngine(
					JSEngineEnum.GRAALVM_JS.getEngineName(),
					"",
					null,
					null,
					null,
					null,
					null,
					null,
					true));
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"engine",engine);
			tapdataEvent = mock(TapdataEvent.class);
			tapEvent = mock(TapUpdateRecordEvent.class);
			when(((TapBaseEvent) tapEvent).getTableId()).thenReturn("tableId");
			when(tapdataEvent.getTapEvent()).thenReturn(tapEvent);
		}
		@Test
		@SneakyThrows
		void testForHandleBefore(){
			Map<String, Object> after = mock(HashMap.class);
			Map<String, Object> before = mock(HashMap.class);
			when(tapEvent.getAfter()).thenReturn(after);
			when(tapEvent.getBefore()).thenReturn(before);
			hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);
			verify(engine,new Times(2)).invokeFunction(anyString(),any());
		}
		@Test
		@SneakyThrows
		void testForHandleWithoutBefore(){
			Map<String, Object> after = mock(HashMap.class);
			when(tapEvent.getAfter()).thenReturn(after);
			when(tapEvent.getBefore()).thenReturn(null);
			hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);
			verify(engine,new Times(1)).invokeFunction(anyString(),any());
		}
	}
}
