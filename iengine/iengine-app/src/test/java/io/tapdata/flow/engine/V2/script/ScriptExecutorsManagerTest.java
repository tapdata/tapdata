package io.tapdata.flow.engine.V2.script;

import base.BaseTest;
import com.hazelcast.core.HazelcastInstance;
import io.tapdata.entity.logger.Log;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;
import org.voovan.tools.collection.CacheMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
	@Nested
	class getScriptExecutorTest{
		private CacheMap<String, ScriptExecutorsManager.ScriptExecutor> cacheMap;
		ScriptExecutorsManager scriptExecutorsManager;
		@BeforeEach
		public void setUp(){
			scriptExecutorsManager = mock(ScriptExecutorsManager.class);
			this.cacheMap = new CacheMap<String, ScriptExecutorsManager.ScriptExecutor>()
					.maxSize(10)
					.autoRemove(true)
					.expire(600)
					.destory((k, v) -> {
						v.close();
						return -1L;
					})
					.create();
			ReflectionTestUtils.setField(scriptExecutorsManager,"cacheMap",cacheMap);
			doCallRealMethod().when(scriptExecutorsManager).getScriptExecutor("test123");
		}
		@DisplayName("test Get script executor normal")
		@Test
		public void testGetScriptExecutorNormal(){
			ScriptExecutorsManager.DummyScriptExecutor dummyScriptExecutor = new ScriptExecutorsManager.DummyScriptExecutor();
			when(scriptExecutorsManager.create("test123")).thenReturn(dummyScriptExecutor);
			ScriptExecutorsManager.ScriptExecutor test123 = scriptExecutorsManager.getScriptExecutor("test123");
			assertEquals(1,cacheMap.size());
			assertEquals(test123,dummyScriptExecutor);
		}
		@DisplayName("test get script executor exception")
		@Test
		public void testGetScriptExecutorNormalException(){
			when(scriptExecutorsManager.create("test123")).thenThrow(new RuntimeException("Create manager faild"));
			IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> {
				ScriptExecutorsManager.ScriptExecutor test123 = scriptExecutorsManager.getScriptExecutor("test123");
			});
			assertEquals(0,cacheMap.size());
			assertEquals("The specified connection source [test123] could not build the executor, please check",illegalArgumentException.getMessage());
		}
	}
}
