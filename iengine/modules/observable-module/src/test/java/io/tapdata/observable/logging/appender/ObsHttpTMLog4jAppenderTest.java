package io.tapdata.observable.logging.appender;

import com.tapdata.mongo.ClientMongoOperator;
import lombok.SneakyThrows;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-01-17 17:12
 **/
@DisplayName("ObsHttpTMLog4jAppender Class Test")
class ObsHttpTMLog4jAppenderTest {
	private ObsHttpTMLog4jAppender obsHttpTMLog4jAppender;
	private ClientMongoOperator clientMongoOperator;

	@BeforeEach
	void setUp() {
		clientMongoOperator = mock(ClientMongoOperator.class);
		obsHttpTMLog4jAppender = new ObsHttpTMLog4jAppender("test", null, null, false, null, clientMongoOperator, 10);
		obsHttpTMLog4jAppender = spy(obsHttpTMLog4jAppender);
	}

	@Test
	@DisplayName("consumeAndInsertLogs method test")
	@SneakyThrows
	void testConsumeAndInsertLogs() {
		doAnswer(invocationOnMock -> {
			Object argument1 = invocationOnMock.getArgument(0);
			assertInstanceOf(List.class, argument1);
			List<String> logs = (List<String>) argument1;
			assertEquals(1, logs.size());
			assertEquals("test log", logs.get(0));
			return null;
		}).when(clientMongoOperator).insertMany(anyList(), eq("MonitoringLogs/batchJson"));
		LogEvent logEvent = mock(LogEvent.class);
		Message message = mock(Message.class);
		when(message.getFormattedMessage()).thenReturn("test log");
		when(logEvent.getMessage()).thenReturn(message);
		obsHttpTMLog4jAppender.append(logEvent);
		TimeUnit.SECONDS.sleep(2);
		verify(clientMongoOperator, times(1)).insertMany(anyList(), eq("MonitoringLogs/batchJson"));
	}

	@Test
	@DisplayName("stop method test")
	void testStop() {
		Object runningObj = ReflectionTestUtils.getField(obsHttpTMLog4jAppender, "running");
		assertInstanceOf(AtomicBoolean.class, runningObj);
		AtomicBoolean running = (AtomicBoolean) runningObj;
		Object consumeMessageThreadPoolObj = ReflectionTestUtils.getField(obsHttpTMLog4jAppender, "consumeMessageThreadPool");
		assertInstanceOf(ExecutorService.class, consumeMessageThreadPoolObj);
		ExecutorService consumeMessageThreadPool = (ExecutorService) consumeMessageThreadPoolObj;

		assertTrue(running.get());
		assertFalse(consumeMessageThreadPool.isShutdown());
		assertFalse(consumeMessageThreadPool.isTerminated());

		obsHttpTMLog4jAppender.stop();

		assertFalse(running.get());
		assertTrue(consumeMessageThreadPool.isShutdown());
		assertTrue(consumeMessageThreadPool.isTerminated());
	}
}
