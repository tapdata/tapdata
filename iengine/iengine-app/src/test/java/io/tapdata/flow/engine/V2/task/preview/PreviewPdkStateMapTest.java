package io.tapdata.flow.engine.V2.task.preview;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-11-06 18:33
 **/
class PreviewPdkStateMapTest {

	private PreviewPdkStateMap previewPdkStateMap;

	@BeforeEach
	void setUp() {
		previewPdkStateMap = new PreviewPdkStateMap();
	}

	@Test
	@DisplayName("test put")
	void test1() {
		previewPdkStateMap.put("key", "value");
		Object mapObj = ReflectionTestUtils.getField(previewPdkStateMap, "map");
		assertInstanceOf(Map.class, mapObj);
		assertEquals(1, ((Map<?, ?>) mapObj).size());
		assertEquals("value", ((Map<?, ?>) mapObj).get("key"));
	}

	@Test
	@DisplayName("test putIfAbsent")
	void test2() {
		previewPdkStateMap.put("key", "value");
		previewPdkStateMap.putIfAbsent("key", "value1");
		Object mapObj = ReflectionTestUtils.getField(previewPdkStateMap, "map");
		assertInstanceOf(Map.class, mapObj);
		assertEquals(1, ((Map<?, ?>) mapObj).size());
		assertEquals("value", ((Map<?, ?>) mapObj).get("key"));
	}

	@Test
	@DisplayName("test remove")
	void test3() {
		previewPdkStateMap.put("key", "value");
		previewPdkStateMap.remove("key");
		Object mapObj = ReflectionTestUtils.getField(previewPdkStateMap, "map");
		assertInstanceOf(Map.class, mapObj);
		assertTrue(((Map<?, ?>) mapObj).isEmpty());
	}

	@Test
	@DisplayName("test clear")
	void test4() {
		previewPdkStateMap.put("key", "value");
		previewPdkStateMap.clear();
		Object mapObj = ReflectionTestUtils.getField(previewPdkStateMap, "map");
		assertInstanceOf(Map.class, mapObj);
		assertTrue(((Map<?, ?>) mapObj).isEmpty());
	}

	@Test
	@DisplayName("test reset")
	void test5() {
		Object mapObj1 = ReflectionTestUtils.getField(previewPdkStateMap, "map");
		previewPdkStateMap.put("key", "value");
		previewPdkStateMap.reset();
		Object mapObj2 = ReflectionTestUtils.getField(previewPdkStateMap, "map");
		assertInstanceOf(Map.class, mapObj2);
		assertTrue(((Map<?, ?>) mapObj2).isEmpty());
		assertNotSame(mapObj1, mapObj2);
	}

	@Test
	@DisplayName("test get")
	void test6() {
		previewPdkStateMap.put("key", "value");
		assertEquals("value", previewPdkStateMap.get("key"));
	}
}