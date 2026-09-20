package com.tapdata.tm.commons.dag;

import com.tapdata.tm.commons.dag.nodes.TableNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("Class DAGNodeTypeInitializer test")
public class DAGNodeTypeInitializerTest {

	private DAGNodeTypeInitializer initializer;
	private ContextRefreshedEvent contextRefreshedEvent;
	private ApplicationContext applicationContext;

	@SuppressWarnings("unchecked")
	private void resetNodeMapping() throws Exception {
		Field nodeMappingField = DAG.class.getDeclaredField("nodeMapping");
		nodeMappingField.setAccessible(true);
		Map<String, Class<? extends Node>> freshMap = new ConcurrentHashMap<>();
		nodeMappingField.set(null, freshMap);
	}

	@BeforeEach
	void setUp() throws Exception {
		resetNodeMapping();
		initializer = new DAGNodeTypeInitializer();
		applicationContext = mock(ApplicationContext.class);
		when(applicationContext.getDisplayName()).thenReturn("MockTestApplicationContext");
		contextRefreshedEvent = new ContextRefreshedEvent(applicationContext);
	}

	@Nested
	@DisplayName("Method onApplicationEvent test")
	class OnApplicationEventTest {

		@Test
		@DisplayName("test onApplicationEvent: should invoke DAG.goInit and register node types after ContextRefreshedEvent")
		void testOnApplicationEventInvokesGoInit() {
			assertTrue(DAG.nodeMapping.isEmpty(), "nodeMapping should be empty before event is handled");

			initializer.onApplicationEvent(contextRefreshedEvent);

			assertFalse(DAG.nodeMapping.isEmpty(), "nodeMapping should be filled after ContextRefreshedEvent is handled");

			Class<? extends Node> tableNodeClass = DAG.getClassByType("table");
			assertNotNull(tableNodeClass, "table node should be registered after event-driven goInit");
			assertEquals(TableNode.class, tableNodeClass, "table node mapping should be TableNode class");

			Class<? extends Node> jsProcessorClass = DAG.getClassByType("js_processor");
			assertNotNull(jsProcessorClass, "js_processor node should be registered after event-driven goInit");
			assertEquals(com.tapdata.tm.commons.dag.process.JsProcessorNode.class, jsProcessorClass,
					"js_processor node mapping should be JsProcessorNode class");

			verify(applicationContext, atLeastOnce()).getDisplayName();
		}

		@Test
		@DisplayName("test onApplicationEvent: multiple ContextRefreshedEvent invocations should keep mapping stable (idempotent)")
		void testOnApplicationEventIdempotent() {
			initializer.onApplicationEvent(contextRefreshedEvent);
			int sizeAfterFirst = DAG.nodeMapping.size();
			Map<String, Class<? extends Node>> snapshot = new ConcurrentHashMap<>(DAG.nodeMapping);

			ApplicationContext anotherCtx = mock(ApplicationContext.class);
			when(anotherCtx.getDisplayName()).thenReturn("AnotherTestApplicationContext");
			ContextRefreshedEvent secondEvent = new ContextRefreshedEvent(anotherCtx);
			initializer.onApplicationEvent(secondEvent);
			initializer.onApplicationEvent(contextRefreshedEvent);
			initializer.onApplicationEvent(secondEvent);

			assertEquals(sizeAfterFirst, DAG.nodeMapping.size(),
					"nodeMapping size should remain stable across multiple ContextRefreshedEvent dispatches");
			assertEquals(snapshot, DAG.nodeMapping,
					"nodeMapping content should remain identical across multiple ContextRefreshedEvent dispatches");
		}

		@Test
		@DisplayName("test onApplicationEvent: event with null applicationContext displayName should not propagate error")
		void testOnApplicationEventWithNullDisplayName() {
			ApplicationContext ctxWithNullName = mock(ApplicationContext.class);
			when(ctxWithNullName.getDisplayName()).thenReturn(null);
			ContextRefreshedEvent eventWithNullName = new ContextRefreshedEvent(ctxWithNullName);

			assertDoesNotThrow(() -> initializer.onApplicationEvent(eventWithNullName),
					"onApplicationEvent should tolerate a null displayName without throwing");
			assertFalse(DAG.nodeMapping.isEmpty(),
					"nodeMapping should still be filled even when context displayName is null");
		}

		@Test
		@DisplayName("test onApplicationEvent: exception thrown from DAG.goInit should be caught and not propagate")
		void testOnApplicationEventCatchesGoInitException() {
			try (MockedStatic<DAG> dagMockedStatic = mockStatic(DAG.class, Mockito.CALLS_REAL_METHODS)) {
				dagMockedStatic.when(DAG::goInit).thenThrow(new RuntimeException("Simulated scan failure"));

				assertDoesNotThrow(() -> initializer.onApplicationEvent(contextRefreshedEvent),
						"exception thrown from goInit should be caught inside onApplicationEvent and must not propagate");

				dagMockedStatic.verify(DAG::goInit, times(1));
			}
		}

		@Test
		@DisplayName("test onApplicationEvent: Error thrown from DAG.goInit must propagate (catch clause only catches Exception)")
		void testOnApplicationEventPropagatesError() {
			try (MockedStatic<DAG> dagMockedStatic = mockStatic(DAG.class, Mockito.CALLS_REAL_METHODS)) {
				dagMockedStatic.when(DAG::goInit).thenThrow(new LinkageError("Simulated linkage error"));

				assertThrows(LinkageError.class, () -> initializer.onApplicationEvent(contextRefreshedEvent),
						"LinkageError thrown from goInit should propagate out of onApplicationEvent (only Exception is caught)");

				dagMockedStatic.verify(DAG::goInit, times(1));
			}
		}

		@Test
		@DisplayName("test onApplicationEvent: event-driven initialization should produce identical result as direct DAG.goInit call")
		void testEventResultMatchesDirectGoInit() throws Exception {
			resetNodeMapping();
			initializer.onApplicationEvent(contextRefreshedEvent);
			Map<String, Class<? extends Node>> eventResult = new ConcurrentHashMap<>(DAG.nodeMapping);

			resetNodeMapping();
			DAG.goInit();
			Map<String, Class<? extends Node>> directResult = new ConcurrentHashMap<>(DAG.nodeMapping);

			assertEquals(directResult.size(), eventResult.size(),
					"event-driven goInit and direct goInit should register the same number of node types");
			assertEquals(directResult, eventResult,
					"event-driven goInit and direct goInit should produce identical node type mappings");
		}
	}
}
