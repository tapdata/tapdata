package com.tapdata.constant;

import com.tapdata.exception.MapUtilException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2024-04-19 12:05
 **/
@DisplayName("Class MapUtil Test")
class MapUtilTest {

	@Nested
	@DisplayName("Method deepCloneMap test")
	class deepCloneMapTest {
		@Test
		@DisplayName("test main process")
		@SneakyThrows
		void testMainProcess() {
			Map<String, Object> map = new HashMap<>();
			map.put("str", "test");
			map.put("int", 1);
			map.put("date", new Date());
			map.put("commonObject", new CommonObject());
			Map<String, Object> newMap = new HashMap<>();

			MapUtil.deepCloneMap(map, newMap);

			assertEquals(map, newMap);
		}

		@Test
		@DisplayName("test embedded map")
		@SneakyThrows
		void testEmbeddedMap() {
			Map<String, Object> subMap = new HashMap<>();
			subMap.put("str", "test");
			subMap.put("int", 1);
			Map<String, Object> map = new HashMap<>();
			map.put("map", subMap);
			Map<String, Object> newMap = new HashMap<>();

			MapUtil.deepCloneMap(map, newMap);

			assertNotSame(map.get("map"), newMap.get("map"));
			assertEquals(map, newMap);
		}

		@Test
		@DisplayName("test embedded list")
		@SneakyThrows
		void testEmbeddedList() {
			List<Object> list = new ArrayList<>();
			list.add("test");
			list.add(1);
			list.add(new Date());
			Map<String, Object> map = new HashMap<>();
			map.put("list", list);
			Map<String, Object> newMap = new HashMap<>();

			MapUtil.deepCloneMap(map, newMap);

			assertNotSame(map.get("list"), newMap.get("list"));
			assertEquals(map, newMap);
		}

		@Test
		@DisplayName("test value is cloneable object")
		@SneakyThrows
		void testCloneableObject() {
			CloneableObject cloneableObject = new CloneableObject(1, "test");
			Map<String, Object> map = new HashMap<>();
			map.put("cloneableObject", cloneableObject);
			Map<String, Object> newMap = new HashMap<>();

			MapUtil.deepCloneMap(map, newMap);

			assertNotSame(map.get("cloneableObject"), newMap.get("cloneableObject"));
			assertEquals(map, newMap);
		}

		@Test
		@DisplayName("test value is cloneable object and not have clone method")
		void testCloneableObjectAndNotHaveCloneMethod() {
			CloneableObject1 cloneableObject = new CloneableObject1(1, "test");
			Map<String, Object> map = new HashMap<>();
			map.put("cloneableObject", cloneableObject);
			Map<String, Object> newMap = new HashMap<>();

			MapUtilException mapUtilException = assertThrows(MapUtilException.class, () -> MapUtil.deepCloneMap(map, newMap));
			assertEquals("No clone method found, class: " + cloneableObject.getClass().getName(), mapUtilException.getMessage());
		}

		@Test
		@DisplayName("test value is cloneable object and clone method throw exception")
		@SneakyThrows
		void testCloneError() {
			CloneableObject cloneableObject = mock(CloneableObject.class);
			RuntimeException runtimeException = new RuntimeException("test");
			when(cloneableObject.clone()).thenThrow(runtimeException);
			Map<String, Object> map = new HashMap<>();
			map.put("cloneableObject", cloneableObject);
			Map<String, Object> newMap = new HashMap<>();

			MapUtilException mapUtilException = assertThrows(MapUtilException.class, () -> MapUtil.deepCloneMap(map, newMap));
			assertEquals(mapUtilException.getCause(), runtimeException);
			assertEquals("Invoke clone method failed, class: " + cloneableObject.getClass().getName(), mapUtilException.getMessage());
		}

		@Test
		@DisplayName("test value is serializable object")
		@SneakyThrows
		void testSerializableObject() {
			SerializableObject serializableObject = new SerializableObject(1, "test");
			Map<String, Object> map = new HashMap<>();
			map.put("serializableObject", serializableObject);
			Map<String, Object> newMap = new HashMap<>();

			MapUtil.deepCloneMap(map, newMap);

			assertNotSame(map.get("serializableObject"), newMap.get("serializableObject"));
			assertEquals(map, newMap);
		}
	}
}