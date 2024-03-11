package io.tapdata.flow.engine.V2.script;

import base.BaseTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.voovan.tools.collection.CacheMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;


public class ScriptExecutorsManagerTest extends BaseTest {

	private CacheMap<String, ScriptExecutorsManager.ScriptExecutor> cacheMap;
	ScriptExecutorsManager scriptExecutorsManager;
	@Before
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
	@Test
	public void testGetScriptExecutorNormal(){
		ScriptExecutorsManager.DummyScriptExecutor dummyScriptExecutor = new ScriptExecutorsManager.DummyScriptExecutor();
		when(scriptExecutorsManager.create("test123")).thenReturn(dummyScriptExecutor);
		ScriptExecutorsManager.ScriptExecutor test123 = scriptExecutorsManager.getScriptExecutor("test123");
		assertEquals(1,cacheMap.size());
		assertEquals(test123,dummyScriptExecutor);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetScriptExecutorNormalException() {
		when(scriptExecutorsManager.create("test123")).thenThrow(new RuntimeException("Create manager faild"));

		ScriptExecutorsManager.ScriptExecutor test123 = scriptExecutorsManager.getScriptExecutor("test123");
		assertEquals(0, cacheMap.size());
	}
}
