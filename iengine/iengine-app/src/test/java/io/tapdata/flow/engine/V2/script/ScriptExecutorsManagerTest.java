package io.tapdata.flow.engine.V2.script;

import base.BaseTest;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.entity.Connections;
import io.tapdata.entity.logger.Log;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-01-24 16:28
 **/
@DisplayName("ScriptExecutorsManager Class Test")
public class ScriptExecutorsManagerTest extends BaseTest {
	ScriptExecutorsManager scriptExecutorsManager;
	private Log log;
	private HazelcastInstance hazelcastInstance;
	private String taskId;
	private String nodeId;

	@BeforeEach
	void setUp() {
		log = mock(Log.class);
		hazelcastInstance = mock(HazelcastInstance.class);
		taskId = "task-1";
		nodeId = "node-1";
		scriptExecutorsManager = new ScriptExecutorsManager(log, mockClientMongoOperator, hazelcastInstance, taskId, nodeId, false);
	}

	@Nested
	@DisplayName("ScriptExecutor inner class test")
	class ScriptExecutorTest {
		@Test
		@DisplayName("test PdkStateMap reset when close")
		void testPdkStateMapResetWhenClose() {
			ScriptExecutorsManager.ScriptExecutor scriptExecutor = mock(ScriptExecutorsManager.ScriptExecutor.class);
			doCallRealMethod().when(scriptExecutor).close();
			PdkStateMap pdkStateMap = mock(PdkStateMap.class);
			ReflectionTestUtils.setField(scriptExecutor, "pdkStateMap", pdkStateMap);
			ReflectionTestUtils.setField(scriptExecutor, "scriptLogger", log);

			scriptExecutor.close();
			verify(pdkStateMap, times(1)).reset();
		}

		@Test
		@DisplayName("test PdkStateMap is null when close")
		void testPdkStateMapIsNullWhenClose() {
			ScriptExecutorsManager.ScriptExecutor scriptExecutor = mock(ScriptExecutorsManager.ScriptExecutor.class);
			doCallRealMethod().when(scriptExecutor).close();
			ReflectionTestUtils.setField(scriptExecutor, "scriptLogger", log);

			Assertions.assertDoesNotThrow(scriptExecutor::close);
		}
	}
}
