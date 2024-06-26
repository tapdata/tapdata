package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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
}
