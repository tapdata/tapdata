package io.tapdata.Schedule;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.worker.WorkerSingletonLock;
import io.tapdata.utils.AppType;
import io.tapdata.utils.UnitTestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/13 11:04 Create
 */
class WorkerHeatBeatReportsTest {
	private static final Logger logger = LogManager.getLogger(WorkerHeatBeatReportsTest.class);
	@Mock
	private ConnectorManager mockConnectorManager;
	@Mock
	private ConfigurationCenter mockConfigCenter;
	@Mock
	private ClientMongoOperator mockClientMongoOperator;

	private WorkerHeatBeatReports workerHeatBeatReports;
	private final AtomicBoolean isCallExit = new AtomicBoolean(false);

	@BeforeEach
	void setUp() {
		MockitoAnnotations.openMocks(this);
		workerHeatBeatReports = new WorkerHeatBeatReports();
	}

	void initWorkerHeatBeatReports(Runnable runnable) {
		try (
			MockedStatic<ConnectorManager> connectorManagerMockedStatic = mockStatic(ConnectorManager.class);
			MockedStatic<WorkerSingletonLock> workerSingletonLockMockedStatic = mockStatic(WorkerSingletonLock.class);
		) {
			isCallExit.set(false);
			connectorManagerMockedStatic.when(() -> ConnectorManager.exit(any())).then(invocationOnMock -> {
				isCallExit.set(true);
				return null;
			});

			String singletonLockCurrentTag = "test-singleton-lock-tag";
			workerSingletonLockMockedStatic.when(WorkerSingletonLock::getCurrentTag).thenReturn(singletonLockCurrentTag);
			workerHeatBeatReports.init(mockConnectorManager, mockConfigCenter, mockClientMongoOperator);

			runnable.run();
		} catch (Exception e) {
			String errMsg = "init " + WorkerHeatBeatReports.class.getSimpleName() + " failed";
			Assertions.fail(errMsg);
			logger.error(errMsg, e);
		}
	}

	@Nested
	class ReportTest {

		void testReport(boolean isDass, boolean isExit, Runnable checkRun) {
			initWorkerHeatBeatReports(() -> {
				try (MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class)) {
					AppType appType = mock(AppType.class);
					when(appType.isDaas()).thenReturn(isDass);
					appTypeMockedStatic.when(AppType::currentType).thenReturn(appType);

					workerHeatBeatReports.report(isExit);

					checkRun.run();
				} catch (Exception e) {
					String errMsg = String.format("test report(%s, %s) failed", isDass, isExit);
					Assertions.fail(errMsg);
					logger.error(errMsg, e);
				}
			});
		}

		@Test
		void daasWithExit() {
			testReport(true, true, () -> {
				verify(mockClientMongoOperator, times(1)).insertOne(any(), eq(ConnectorConstant.WORKER_COLLECTION + "/health"));
				assertTrue(isCallExit.get(), "The exit method should be called here");
			});
		}

		@Test
		void daasWithoutExit() {
			testReport(true, false, () -> {
				verify(mockClientMongoOperator, times(1)).insertOne(any(), eq(ConnectorConstant.WORKER_COLLECTION + "/health"));
				assertFalse(isCallExit.get(), "The exit method can't be called here");
			});
		}

		@Test
		void notDaasWithExit() {
			testReport(false, true, () -> {
				verify(mockClientMongoOperator, times(1)).insertOne(any(), eq(ConnectorConstant.WORKER_COLLECTION + "/health"));
				assertFalse(isCallExit.get(), "The exit method can't be called here");
			});
		}

		@Test
		void notDaasWithoutExit() {
			testReport(false, false, () -> {
				verify(mockClientMongoOperator, times(1)).insertOne(any(), eq(ConnectorConstant.WORKER_COLLECTION + "/health"));
				assertFalse(isCallExit.get(), "The exit method can't be called here");
			});
		}
	}

	@Nested
	class ConfigIfNotBlankTest {

		@BeforeEach
		void setConfigCenter() {
			UnitTestUtils.injectField(WorkerHeatBeatReports.class, workerHeatBeatReports, "configCenter", mockConfigCenter);
		}

		@Test
		void testNotBlank() {
			String key = "test-config-not-blank";
			String val = "test-config-not-blank-value";
			when(mockConfigCenter.getConfig(key)).thenReturn(val);

			AtomicBoolean isCalledSetter = new AtomicBoolean(false);
			workerHeatBeatReports.configIfNotBlank(key, (k, v) -> {
				assertEquals(key, k);
				assertEquals(val, v);
				isCalledSetter.set(true);
			});
			assertTrue(isCalledSetter.get());
		}

		@Test
		void testBlank() {
			UnitTestUtils.injectField(WorkerHeatBeatReports.class, workerHeatBeatReports, "configCenter", mockConfigCenter);

			String key = "test-config-blank";
			when(mockConfigCenter.getConfig(key)).thenReturn("   	");

			AtomicBoolean isCalledSetter = new AtomicBoolean(false);
			workerHeatBeatReports.configIfNotBlank(key, (k, v) -> isCalledSetter.set(true));
			assertFalse(isCalledSetter.get());
		}

		@Test
		void testBlankWithNull() {
			UnitTestUtils.injectField(WorkerHeatBeatReports.class, workerHeatBeatReports, "configCenter", mockConfigCenter);

			String key = "test-config-blank-with-null";
			when(mockConfigCenter.getConfig(key)).thenReturn(null);

			AtomicBoolean isCalledSetter = new AtomicBoolean(false);
			workerHeatBeatReports.configIfNotBlank(key, (k, v) -> isCalledSetter.set(true));
			assertFalse(isCalledSetter.get());
		}
	}

}
