package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.logger.TapLogger;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-22 10:28
 **/
@DisplayName("HazelcastPdkBaseNode CLass Test")
class HazelcastPdkBaseNodeTest extends BaseHazelcastNodeTest {
	HazelcastPdkBaseNode hazelcastPdkBaseNode;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		hazelcastPdkBaseNode = new HazelcastPdkBaseNode(dataProcessorContext) {
		};
	}

	@Nested
	class FunctionRetryQueryTest {
		long timestamp;
		@BeforeEach
		void init() {
			hazelcastPdkBaseNode = mock(HazelcastPdkBaseNode.class);
			timestamp = System.currentTimeMillis();
			when(hazelcastPdkBaseNode.functionRetryQuery(anyLong(), anyBoolean())).thenCallRealMethod();
		}

		/**signFunction*/
		@Test
		void functionRetryQueryNormal() {
			Update update = hazelcastPdkBaseNode.functionRetryQuery(timestamp, true);
			Document document = assertResult(update);
			Assertions.assertTrue(document.containsKey("taskRetryStartTime"));
			Assertions.assertEquals(timestamp, document.get("taskRetryStartTime"));
		}

		/**clearFunction*/
		@Test
		void functionRetryQueryNotSingle() {
			Update update = hazelcastPdkBaseNode.functionRetryQuery(timestamp, false);
			Document document = assertResult(update);
			Assertions.assertTrue(document.containsKey("functionRetryEx"));
			Assertions.assertEquals(timestamp + 5 * 60 * 1000L, document.get("functionRetryEx"));
			Assertions.assertTrue(document.containsKey("taskRetryStartTime"));
			Assertions.assertEquals(0, document.get("taskRetryStartTime"));
		}

		Document assertResult(Update update) {
			Assertions.assertNotNull(update);
			Document document = update.getUpdateObject();
			Assertions.assertNotNull(document);
			Assertions.assertTrue(document.containsKey("$set"));
			Object set = document.get("$set");
			Assertions.assertEquals(Document.class, set.getClass());
			Document setMap = (Document) set;
			Assertions.assertTrue(setMap.containsKey("functionRetryStatus"));
			Assertions.assertEquals(TaskDto.RETRY_STATUS_RUNNING, setMap.get("functionRetryStatus"));
			return setMap;
		}
	}

	@Test
	@DisplayName("DoInit method test")
	void testDoInit() {
		AtomicReference<String> log = new AtomicReference<>();
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).debug(anyString());
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).info(anyString());
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).warn(anyString());
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).error(anyString());
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).fatal(anyString());
		ReflectionTestUtils.setField(hazelcastPdkBaseNode, "obsLogger", mockObsLogger);
		hazelcastPdkBaseNode.doInit(jetContext);
		Object actualObj = ReflectionTestUtils.getField(hazelcastPdkBaseNode, "logListener");
		assertNotNull(actualObj);
		assertTrue(actualObj instanceof TapLogger.LogListener);
		((TapLogger.LogListener) actualObj).debug("debug test");
		assertEquals("debug test", log.get());
		((TapLogger.LogListener) actualObj).info("info test");
		assertEquals("info test", log.get());
		((TapLogger.LogListener) actualObj).warn("warn test");
		assertEquals("warn test", log.get());
		((TapLogger.LogListener) actualObj).error("error test");
		assertEquals("error test", log.get());
		((TapLogger.LogListener) actualObj).fatal("fatal test");
		assertEquals("fatal test", log.get());
		((TapLogger.LogListener) actualObj).memory("memory test");
		assertEquals("memory test", log.get());
	}
}
