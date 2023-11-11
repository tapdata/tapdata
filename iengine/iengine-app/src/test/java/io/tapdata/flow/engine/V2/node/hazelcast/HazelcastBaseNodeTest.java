package io.tapdata.flow.engine.V2.node.hazelcast;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.error.TapProcessorUnknownException;
import io.tapdata.exception.TapCodeException;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2023-11-11 15:41
 **/
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class HazelcastBaseNodeTest {

	private HazelcastBaseNode hazelcastBaseNode;

	@BeforeEach
	void beforeEach() {
		ProcessorBaseContext processorBaseContext = mock(ProcessorBaseContext.class);
		hazelcastBaseNode = new HazelcastBaseNode(processorBaseContext) {
		};
	}

	@Nested
	@Order(1)
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
	@Order(2)
	class SetThreadNameTest {
		private Method setThreadNameMethod;

		@BeforeEach
		void beforeEach() throws NoSuchMethodException {
			setThreadNameMethod = HazelcastBaseNode.class.getDeclaredMethod("setThreadName");
			setThreadNameMethod.setAccessible(true);
		}

		@Test
		void testSetThreadName() throws InvocationTargetException, IllegalAccessException {
			TableNode node = mock(TableNode.class);
			TaskDto taskDto = mock(TaskDto.class);
			ProcessorBaseContext processorBaseContext = hazelcastBaseNode.getProcessorBaseContext();
			when(node.getId()).thenReturn("59a335e8-7c17-40c5-8295-25023d6c4398");
			when(node.getName()).thenReturn("junit-node");
			when(taskDto.getId()).thenReturn(new ObjectId("655231353d2fb23e8251ad9d"));
			when(taskDto.getName()).thenReturn("Junit-task");
			when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
			when(processorBaseContext.getNode()).thenReturn((Node) node);
			setThreadNameMethod.invoke(hazelcastBaseNode);
			String actual = Thread.currentThread().getName();
			assertEquals("HazelcastBaseNode-Junit-task(655231353d2fb23e8251ad9d)-junit-node(59a335e8-7c17-40c5-8295-25023d6c4398)", actual);
		}
	}
}
