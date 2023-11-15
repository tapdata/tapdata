package io.tapdata.flow.engine.V2.node.hazelcast;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.TapProcessorUnknownException;
import io.tapdata.exception.TapCodeException;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2023-11-11 15:41
 **/
@RunWith(MockitoJUnitRunner.class)
class HazelcastBaseNodeTest {

	public static final String TEST_TABLE = "test_table";
	protected ProcessorBaseContext processorBaseContext;
	protected TableNode tableNode;
	protected TaskDto taskDto;
	private HazelcastBaseNode hazelcastBaseNode;
	private AspectManager aspectManager;

	@BeforeEach
	void beforeEach() {
		// Mock some common object
		tableNode = mock(TableNode.class);
		taskDto = mock(TaskDto.class);
		processorBaseContext = mock(ProcessorBaseContext.class);
		hazelcastBaseNode = new HazelcastBaseNode(processorBaseContext) {
		};
		aspectManager = InstanceFactory.instance(AspectManager.class);

		// Mock data
		when(tableNode.getId()).thenReturn("59a335e8-7c17-40c5-8295-25023d6c4398");
		when(tableNode.getName()).thenReturn("junit-node");
		when(taskDto.getId()).thenReturn(new ObjectId("655231353d2fb23e8251ad9d"));
		when(taskDto.getName()).thenReturn("Junit-task");
		when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
		when(processorBaseContext.getNode()).thenReturn((Node) tableNode);
	}

	@Nested
	class HazelcastBaseNodeInitTest {
		@Test
		void testHazelcastBaseNodeDefaultVariables() {
			assertNotNull(hazelcastBaseNode.getProcessorBaseContext());
			ProcessorBaseContext actualProcessorBaseContext = hazelcastBaseNode.getProcessorBaseContext();
			assertEquals(processorBaseContext, actualProcessorBaseContext);
			assertNotNull(hazelcastBaseNode.getNode());
		}
	}

	@Nested
	class ExecuteDAspectTest {

		private AtomicBoolean consumed;
		private AtomicBoolean observed;
		private AlwaysIntercept alwaysIntercept;
		private NeverIntercept neverIntercept;
		private AspectObserver<MockDataFunctionAspect> aspectObserver;

		@BeforeEach
		void beforeEach() {
			// mock aspect, observer, interceptor
			consumed = new AtomicBoolean(false);
			observed = new AtomicBoolean(false);
			aspectObserver = aspect -> observed.compareAndSet(false, true);
			alwaysIntercept = new AlwaysIntercept();
			neverIntercept = new NeverIntercept();
		}

		@Test
		void testExecuteDataFuncAspectNotIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, neverIntercept);
			AspectInterceptResult actual = hazelcastBaseNode.executeDataFuncAspect(
					MockDataFunctionAspect.class,
					MockDataFunctionAspect::new,
					mockAspect -> consumed.compareAndSet(false, true)
			);
			assertNotNull(actual);
			assertFalse(actual.isIntercepted());
			assertTrue(consumed.get());
			assertTrue(observed.get());
		}

		@Test
		void testExecuteDataFuncAspectIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, alwaysIntercept);
			AspectInterceptResult actual = hazelcastBaseNode.executeDataFuncAspect(
					MockDataFunctionAspect.class,
					MockDataFunctionAspect::new,
					mockAspect -> consumed.compareAndSet(false, true)
			);
			assertNotNull(actual);
			assertTrue(actual.isIntercepted());
			assertFalse(consumed.get());
			assertFalse(observed.get());
		}

		@Test
		void testExecuteDataFuncAspectWithoutIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			AspectInterceptResult actual = hazelcastBaseNode.executeDataFuncAspect(
					MockDataFunctionAspect.class,
					MockDataFunctionAspect::new,
					mockAspect -> consumed.compareAndSet(false, true)
			);
			assertNull(actual);
			assertTrue(consumed.get());
			assertTrue(observed.get());
		}

		@Test
		void testExecuteDataFuncAspectWithoutObserve() {
			AspectInterceptResult actual = hazelcastBaseNode.executeDataFuncAspect(
					MockDataFunctionAspect.class,
					MockDataFunctionAspect::new,
					mockAspect -> consumed.compareAndSet(false, true)
			);
			assertNull(actual);
			assertTrue(consumed.get());
			assertFalse(observed.get());
		}

		@Test
		void testExecuteAspectOneParamNotIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, neverIntercept);
			MockDataFunctionAspect mockDataFunctionAspect = new MockDataFunctionAspect();
			AspectInterceptResult actual = hazelcastBaseNode.executeAspect(mockDataFunctionAspect);
			assertNotNull(actual);
			assertFalse(actual.isIntercepted());
			assertTrue(observed.get());
		}

		@Test
		void testExecuteAspectOneParamIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, alwaysIntercept);
			MockDataFunctionAspect mockDataFunctionAspect = new MockDataFunctionAspect();
			AspectInterceptResult actual = hazelcastBaseNode.executeAspect(mockDataFunctionAspect);
			assertNotNull(actual);
			assertTrue(actual.isIntercepted());
			assertFalse(observed.get());
		}

		@Test
		void testExecuteAspectTwoParamNotIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, neverIntercept);
			AspectInterceptResult actual = hazelcastBaseNode.executeAspect(MockDataFunctionAspect.class, MockDataFunctionAspect::new);
			assertNull(actual);
			assertTrue(observed.get());
		}

		@Test
		void testExecuteAspectTwoParamIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, alwaysIntercept);
			AspectInterceptResult actual = hazelcastBaseNode.executeAspect(MockDataFunctionAspect.class, MockDataFunctionAspect::new);
			assertNotNull(actual);
			assertTrue(actual.isIntercepted());
			assertFalse(observed.get());
		}

		@AfterEach
		void afterEach() {
			aspectManager.unregisterAspectObserver(MockDataFunctionAspect.class, aspectObserver);
			aspectManager.unregisterAspectInterceptor(MockDataFunctionAspect.class, alwaysIntercept);
			aspectManager.unregisterAspectInterceptor(MockDataFunctionAspect.class, neverIntercept);
		}
	}

	@Nested
	class WrapTapCodeExceptionMethodTest {

		private Method wrapTapCodeExceptionMethod;

		@BeforeEach
		void beforeEach() throws NoSuchMethodException {
			wrapTapCodeExceptionMethod = HazelcastBaseNode.class.getDeclaredMethod("wrapTapCodeException", Throwable.class);
			wrapTapCodeExceptionMethod.setAccessible(true);
		}

		@Test
		void testWrapTapCodeExceptionInputTapCodeException() throws Throwable {
			TapCodeException tapCodeException = new TapCodeException("test");
			Object actual = invokeWrapTapCodeExceptionMethod(tapCodeException);
			assertNotNull(actual);
			assertEquals(tapCodeException, actual);
		}

		@Test
		void testWrapTapCodeExceptionInputRuntimeException() throws Throwable {
			RuntimeException runtimeException = new RuntimeException();
			Object actual = invokeWrapTapCodeExceptionMethod(runtimeException);
			assertNotNull(actual);
			assertEquals(TapProcessorUnknownException.class, actual.getClass());
			assertNotNull(((TapProcessorUnknownException) actual).getCause());
			assertEquals(runtimeException, ((TapProcessorUnknownException) actual).getCause());
		}

		@Test
		void testWrapTapCodeExceptionInputNull() {
			Throwable actual = assertThrows(IllegalArgumentException.class, () -> invokeWrapTapCodeExceptionMethod((Object) null));
			assertNotNull(actual.getMessage());
			assertEquals("Input exception cannot be null", actual.getMessage());
		}

		Object invokeWrapTapCodeExceptionMethod(Object... args) throws Throwable {
			try {
				return wrapTapCodeExceptionMethod.invoke(null, args);
			} catch (InvocationTargetException e) {
				throw e.getTargetException();
			}
		}
	}

	@Nested
	class SetThreadNameTest {
		private Method setThreadNameMethod;

		@BeforeEach
		void beforeEach() throws NoSuchMethodException {
			setThreadNameMethod = HazelcastBaseNode.class.getDeclaredMethod("setThreadName");
			setThreadNameMethod.setAccessible(true);
		}

		@Test
		void testSetThreadName() throws InvocationTargetException, IllegalAccessException {
			setThreadNameMethod.invoke(hazelcastBaseNode);
			String actual = Thread.currentThread().getName();
			assertEquals("HazelcastBaseNode-Junit-task(655231353d2fb23e8251ad9d)-junit-node(59a335e8-7c17-40c5-8295-25023d6c4398)", actual);
		}
	}

	@Nested
	class InitFilterCodecTest {
		@Test
		void testInitFilterCodec() {
			Object initFilterCodecObj = ReflectionTestUtils.invokeMethod(hazelcastBaseNode, "initFilterCodec");
			assertNotNull(initFilterCodecObj);
			assertEquals(TapCodecsFilterManager.class, initFilterCodecObj.getClass());
		}
	}

	@Nested
	class TransformFromTapValueTest {
		final Long TIME = 1700015312781L;
		final Map<String, Object> MOCK_DATA = new HashMap<String, Object>() {{
			put("id", new TapNumberValue(1D));
			put("name", new TapStringValue("test"));
			put("insert_dte", new TapDateTimeValue(new DateTime(TIME, 3)));
		}};
		TapInsertRecordEvent tapInsertRecordEvent;
		TapUpdateRecordEvent tapUpdateRecordEvent;

		@BeforeEach
		void beforeEach() {
			tapInsertRecordEvent = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent.getAfter()).thenReturn(MOCK_DATA);
			when(tapInsertRecordEvent.getTableId()).thenReturn(TEST_TABLE);
			when(tapInsertRecordEvent.getReferenceTime()).thenReturn(TIME);

			tapUpdateRecordEvent = mock(TapUpdateRecordEvent.class);
			when(tapUpdateRecordEvent.getBefore()).thenReturn(MOCK_DATA);
			when(tapUpdateRecordEvent.getAfter()).thenReturn(MOCK_DATA);
			when(tapUpdateRecordEvent.getTableId()).thenReturn(TEST_TABLE);
			when(tapUpdateRecordEvent.getReferenceTime()).thenReturn(TIME);
		}

		@Test
		void testTransformFromTapValueInputTapInsertRecordEvent() {
			ReflectionTestUtils.setField(hazelcastBaseNode, "codecsFilterManager", ReflectionTestUtils.invokeMethod(hazelcastBaseNode, "initFilterCodec"));
			assertNotNull(ReflectionTestUtils.getField(hazelcastBaseNode, "codecsFilterManager"));
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			Object actualObj = ReflectionTestUtils.invokeMethod(hazelcastBaseNode, "transformFromTapValue", tapdataEvent);
			assertNotNull(actualObj);
			assertEquals(HazelcastBaseNode.TapValueTransform.class, actualObj.getClass());
			HazelcastBaseNode.TapValueTransform tapValueTransform = (HazelcastBaseNode.TapValueTransform) actualObj;
			assertNotNull(tapValueTransform.getAfter());
			assertNull(tapValueTransform.getBefore());
			System.out.println(tapValueTransform.getAfter());
		}
	}
}
