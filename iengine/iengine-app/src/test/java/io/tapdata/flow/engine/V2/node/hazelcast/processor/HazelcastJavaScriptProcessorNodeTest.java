package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTest;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.Log4jScriptLogger;
import com.tapdata.processor.LoggingOutputStream;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.MockTaskUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2023-11-21 22:26
 **/
class HazelcastJavaScriptProcessorNodeTest extends BaseTest {
	private ProcessorBaseContext processorBaseContext;
	private TableNode tableNode;
	private TaskDto taskDto;
	private HazelcastJavaScriptProcessorNode hazelcastJavaScriptProcessorNode;
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNodeTest.class);

	@BeforeEach
	void beforeEach() {
		// Mock task and node data
		taskDto = MockTaskUtil.setUpTaskDtoByJsonFile();
		tableNode = (TableNode) taskDto.getDag().getNodes().get(0);

		// Mock some common object
		processorBaseContext = mock(ProcessorBaseContext.class);
		when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
		when(processorBaseContext.getNode()).thenReturn((Node) tableNode);

		hazelcastJavaScriptProcessorNode = new HazelcastJavaScriptProcessorNode(processorBaseContext) {
		};
	}

	@Test
	void testLog() {
		try (Context context = Context.newBuilder("js")
				.allowAllAccess(true)
				.option("engine.WarnInterpreterOnly", "false")
				.logHandler(System.err)
				.out(new LoggingOutputStream(new Log4jScriptLogger(logger), Level.INFO)).build()) {

			context.eval("js", "console.info('fdasfdsafdsafad')");
		}
	}

	@Test
	void testLog1() throws ScriptException {

		Thread.currentThread().setName("test-->");

		Engine engine = Engine.newBuilder()
				.allowExperimentalOptions(true)
				.option("engine.WarnInterpreterOnly", "false")
				.build();
		GraalJSScriptEngine graalJSScriptEngine = GraalJSScriptEngine
				.create(engine,
						Context.newBuilder("js")
								.allowAllAccess(true)
								.allowHostAccess(HostAccess.newBuilder(HostAccess.ALL)
										.targetTypeMapping(Value.class, Object.class,
												v -> v.hasArrayElements() && v.hasMembers(), v -> v.as(List.class)).build())
				);


		SimpleScriptContext ctxt = new SimpleScriptContext();
		ctxt.setWriter(new OutputStreamWriter(new LoggingOutputStream(new Log4jScriptLogger(logger), Level.INFO)));
		graalJSScriptEngine.eval("console.info('fdasfdsafdsafad')", ctxt);

		graalJSScriptEngine.close();
	}

	@Nested
	class DoCloseTest {

		@BeforeEach
		void beforeEach() {
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode, "obsLogger", mockObsLogger);
		}

		@Test
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
