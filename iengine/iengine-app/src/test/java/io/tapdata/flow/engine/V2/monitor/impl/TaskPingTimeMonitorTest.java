package io.tapdata.flow.engine.V2.monitor.impl;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.util.ConsumerImpl;
import io.tapdata.flow.engine.V2.util.SupplierImpl;
import io.tapdata.utils.AppType;
import io.tapdata.utils.UnitTestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.lang.reflect.Field;

import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/12 16:48 Create
 */
class TaskPingTimeMonitorTest {
	SupplierImpl<Boolean> stopTask;
	ConsumerImpl<TerminalMode> taskMonitor;
	HttpClientMongoOperator clientMongoOperator;
	TaskPingTimeMonitor taskPingTimeMonitor;


	@BeforeEach
	public void setup() {
		stopTask = mock(SupplierImpl.class);
		taskMonitor = mock(ConsumerImpl.class);
		clientMongoOperator = mock(HttpClientMongoOperator.class);

		taskPingTimeMonitor = mock(TaskPingTimeMonitor.class, CALLS_REAL_METHODS);
		UnitTestUtils.injectField(TaskPingTimeMonitor.class, taskPingTimeMonitor, "logger", LogManager.getLogger(TaskPingTimeMonitor.class));
		UnitTestUtils.injectField(TaskPingTimeMonitor.class, taskPingTimeMonitor, "stopTask", stopTask);
		UnitTestUtils.injectField(TaskPingTimeMonitor.class, taskPingTimeMonitor, "taskMonitor", taskMonitor);
		UnitTestUtils.injectField(TaskPingTimeMonitor.class, taskPingTimeMonitor, "clientMongoOperator", clientMongoOperator);
	}

	@Nested
	class TaskPingTimeUseHttpTest {

		@Test
		void whenUpdateResultModifiedCountIsOne() {
			Query query = mock(Query.class);
			Update update = mock(Update.class);

			UpdateResult updateResult = mock(UpdateResult.class);
			when(updateResult.getModifiedCount()).thenReturn(1L);
			when(clientMongoOperator.update(any(), any(), any())).thenReturn(updateResult);

			taskPingTimeMonitor.taskPingTimeUseHttp(query, update);
		}

		@Test
		void whenUpdateResultModifiedCountIsZero() {
			Query query = mock(Query.class);
			Update update = mock(Update.class);

			UpdateResult updateResult = mock(UpdateResult.class);
			when(updateResult.getModifiedCount()).thenReturn(0L);
			when(clientMongoOperator.update(any(), any(), any())).thenReturn(updateResult);

			taskPingTimeMonitor.taskPingTimeUseHttp(query, update);

			verify(taskMonitor, times(1)).accept(TerminalMode.INTERNAL_STOP);
			verify(stopTask, times(1)).get();
		}

		@Test
		void whenUpdateResultModifiedCountIsZeroAndIsCloud() {
			Query query = mock(Query.class);
			Update update = mock(Update.class);

			UpdateResult updateResult = mock(UpdateResult.class);
			when(updateResult.getModifiedCount()).thenReturn(0L);
			when(clientMongoOperator.update(any(), any(), any())).thenReturn(updateResult);

			try (MockedStatic<AppType> ignoreAppType = mockStatic(AppType.class)) {
				AppType appType = mock(AppType.class);
				when(appType.isCloud()).thenReturn(true);
				ignoreAppType.when(AppType::currentType).thenReturn(appType);

				taskPingTimeMonitor.taskPingTimeUseHttp(query, update);
			}
		}

		@Test
		void whenUpdateException() {
			Query query = mock(Query.class);
			Update update = mock(Update.class);

			// Ignore log printing for successful use cases
			UnitTestUtils.injectField(TaskPingTimeMonitor.class, taskPingTimeMonitor, "logger", mock(Logger.class));
			taskPingTimeMonitor.taskPingTimeUseHttp(query, update);

			verify(taskMonitor, times(1)).accept(TerminalMode.INTERNAL_STOP);
			verify(stopTask, times(1)).get();
		}

		@Test
		void whenUpdateExceptionAndIsCloud() {
			Query query = mock(Query.class);
			Update update = mock(Update.class);

			try (MockedStatic<AppType> ignoreAppType = mockStatic(AppType.class)) {
				AppType appType = mock(AppType.class);
				when(appType.isCloud()).thenReturn(true);
				ignoreAppType.when(AppType::currentType).thenReturn(appType);

				taskPingTimeMonitor.taskPingTimeUseHttp(query, update);
			}
		}

	}
}
