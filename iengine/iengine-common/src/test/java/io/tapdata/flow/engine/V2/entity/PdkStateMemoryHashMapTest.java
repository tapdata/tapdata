package io.tapdata.flow.engine.V2.entity;

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
class PdkStateMemoryHashMapTest {

	private PdkStateMemoryHashMap pdkStateMemoryHashMap;

	@BeforeEach
	void setUp() {
		pdkStateMemoryHashMap = new PdkStateMemoryHashMap();
	}

	@Test
	@DisplayName("test put")
	void test1() {
		pdkStateMemoryHashMap.put("key", "value");
		Object mapObj = ReflectionTestUtils.getField(pdkStateMemoryHashMap, "map");
		assertInstanceOf(Map.class, mapObj);
		assertEquals(1, ((Map<?, ?>) mapObj).size());
		assertEquals("value", ((Map<?, ?>) mapObj).get("key"));
	}

	@Test
	@DisplayName("test putIfAbsent")
	void test2() {
		pdkStateMemoryHashMap.put("key", "value");
		pdkStateMemoryHashMap.putIfAbsent("key", "value1");
		Object mapObj = ReflectionTestUtils.getField(pdkStateMemoryHashMap, "map");
		assertInstanceOf(Map.class, mapObj);
		assertEquals(1, ((Map<?, ?>) mapObj).size());
		assertEquals("value", ((Map<?, ?>) mapObj).get("key"));
	}

	@Test
	@DisplayName("test remove")
	void test3() {
		pdkStateMemoryHashMap.put("key", "value");
		pdkStateMemoryHashMap.remove("key");
		Object mapObj = ReflectionTestUtils.getField(pdkStateMemoryHashMap, "map");
		assertInstanceOf(Map.class, mapObj);
		assertTrue(((Map<?, ?>) mapObj).isEmpty());
	}

	@Test
	@DisplayName("test clear")
	void test4() {
		pdkStateMemoryHashMap.put("key", "value");
		pdkStateMemoryHashMap.clear();
		Object mapObj = ReflectionTestUtils.getField(pdkStateMemoryHashMap, "map");
		assertInstanceOf(Map.class, mapObj);
		assertTrue(((Map<?, ?>) mapObj).isEmpty());
	}

	@Test
	@DisplayName("test reset")
	void test5() {
		Object mapObj1 = ReflectionTestUtils.getField(pdkStateMemoryHashMap, "map");
		pdkStateMemoryHashMap.put("key", "value");
		pdkStateMemoryHashMap.reset();
		Object mapObj2 = ReflectionTestUtils.getField(pdkStateMemoryHashMap, "map");
		assertInstanceOf(Map.class, mapObj2);
		assertTrue(((Map<?, ?>) mapObj2).isEmpty());
		assertNotSame(mapObj1, mapObj2);
	}

	@Test
	@DisplayName("test get")
	void test6() {
		pdkStateMemoryHashMap.put("key", "value");
		assertEquals("value", pdkStateMemoryHashMap.get("key"));
	}
}
