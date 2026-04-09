package com.tapdata.constant;

import com.tapdata.exception.MapUtilException;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
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

	@Nested
	@DisplayName("Method getValueByKey test")
	class getValueByKeyTest {
		@Test
		@DisplayName("test nested map and edge cases")
		void testNestedAndEdge() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", new HashMap<>(Map.of("b", 1)));
			map.put("x", 2);
			assertEquals(1, MapUtil.getValueByKey(map, "a.b"));
			assertNull(MapUtil.getValueByKey(map, "a.c"));
			assertEquals(2, MapUtil.getValueByKey(map, "x"));
			assertNull(MapUtil.getValueByKey(map, ".a"));
			assertNull(MapUtil.getValueByKey(map, "a."));
			assertNull(MapUtil.getValueByKey(null, "x"));
			assertNull(MapUtil.getValueByKey(new HashMap<>(), "x"));
			assertNull(MapUtil.getValueByKey(map, ""));
		}

		@Test
		@DisplayName("test replacement for special chars")
		void testReplacement() {
			Map<String, Object> map = new HashMap<>();
			map.put("_id", 1);
			map.put("a_b", 2);
			map.put("a_b_", 3);
			assertEquals(1, MapUtil.getValueByKey(map, "$id", "_"));
			assertEquals(2, MapUtil.getValueByKey(map, "a b", "_"));
			assertEquals(3, MapUtil.getValueByKey(map, "a.b.", "_"));
		}
	}

	@Nested
	@DisplayName("Method needSplit test")
	class needSplitTest {
		@Test
		void testNeedSplit() {
			assertFalse(MapUtil.needSplit("a"));
			assertFalse(MapUtil.needSplit(".a"));
			assertFalse(MapUtil.needSplit("a."));
			assertTrue(MapUtil.needSplit("a.b"));
		}
	}

	@Nested
	@DisplayName("Method removeValueByKey / containsKey / getValuePositionInMap test")
	class removeContainsPositionTest {
		@Test
		void testRemoveAndContainsAndPosition() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", new HashMap<>(Map.of("b", 1)));
			map.put("_id", 2);

			assertTrue(MapUtil.containsKey(map, "a.b"));
			assertFalse(MapUtil.containsKey(map, "a.c"));
			assertEquals(2, MapUtil.getValuePositionInMap(map, "a.b"));
			assertEquals(0, MapUtil.getValuePositionInMap(map, "a.c"));

			assertTrue(MapUtil.removeValueByKey(map, "a.b"));
			assertFalse(MapUtil.containsKey(map, "a.b"));
			assertFalse(MapUtil.removeValueByKey(map, "a.b"));

			assertTrue(MapUtil.removeValueByKey(map, "$id", "_"));
			assertFalse(map.containsKey("_id"));
		}
	}

	@Nested
	@DisplayName("Method putValueInMap test")
	class putValueInMapTest {
		@Test
		@SneakyThrows
		void testPutValueInMapCreateNested() {
			Map<String, Object> map = new HashMap<>();
			MapUtil.putValueInMap(map, "a.b", 1, "");
			assertEquals(1, ((Map<?, ?>) map.get("a")).get("b"));

			MapUtil.putValueInMap(map, "$id", 2, "_");
			assertEquals(2, map.get("_id"));
		}

		@Test
		@SneakyThrows
		void testPutValueInMapWriteToListOfMaps() {
			Map<String, Object> map = new HashMap<>();
			List<Object> list = new ArrayList<>();
			list.add(new HashMap<>(Map.of("x", 1)));
			list.add(new HashMap<>(Map.of("y", 2)));
			map.put("l", list);

			MapUtil.putValueInMap(map, "l.k", "v", "");
			assertEquals("v", ((Map<?, ?>) ((List<?>) map.get("l")).get(0)).get("k"));
			assertEquals("v", ((Map<?, ?>) ((List<?>) map.get("l")).get(1)).get("k"));
		}
	}

	@Nested
	@DisplayName("Method copyToNewMap / copyToNewDocument test")
	class copyToNewMapTest {
		@Test
		void testCopyToNewMapShouldCopyListAndNestedMap() {
			Map<String, Object> nested = new LinkedHashMap<>();
			nested.put("b", 1);
			List<Object> list = new ArrayList<>(List.of(1, 2));
			Map<String, Object> map = new HashMap<>();
			map.put("a", nested);
			map.put("l", list);

			Map<String, Object> newMap = new HashMap<>();
			MapUtil.copyToNewMap(map, newMap);

			assertNotSame(map.get("l"), newMap.get("l"));
			assertEquals(map.get("l"), newMap.get("l"));
			assertNotSame(map.get("a"), newMap.get("a"));
			assertEquals(map.get("a"), newMap.get("a"));
			assertInstanceOf(LinkedHashMap.class, newMap.get("a"));
		}

		@Test
		void testCopyToNewDocumentShouldUseDocumentForNestedMap() {
			Map<String, Object> nested = new HashMap<>();
			nested.put("b", 1);
			Map<String, Object> map = new HashMap<>();
			map.put("a", nested);

			Map<String, Object> newMap = new HashMap<>();
			MapUtil.copyToNewDocument(map, newMap);

			assertInstanceOf(org.bson.Document.class, newMap.get("a"));
			assertEquals(1, ((Map<?, ?>) newMap.get("a")).get("b"));
		}
	}

	@Nested
	@DisplayName("Method obj2Map / obj2Document test")
	class obj2MapTest {
		static class Obj {
			private String a = "x";
			private Integer b = null;
			@io.tapdata.annotation.Ignore
			private String c = "ignored";
		}

		@Test
		@SneakyThrows
		void testObj2Map() {
			Map<String, Object> map = MapUtil.obj2Map(new Obj());
			assertEquals("x", map.get("a"));
			assertFalse(map.containsKey("b"));
			assertEquals("ignored", map.get("c"));
		}

		@Test
		@SneakyThrows
		void testObj2DocumentShouldSkipIgnoreAndJacocoField() {
			Obj obj = new Obj();
			org.bson.Document doc = MapUtil.obj2Document(obj);
			assertEquals("x", doc.get("a"));
			assertFalse(doc.containsKey("b"));
			assertFalse(doc.containsKey("c"));

			org.bson.Document doc2 = new org.bson.Document();
			doc2.put("$jacocoData", 1);
			assertTrue(doc2.containsKey("$jacocoData"));
		}
	}

	@Nested
	@DisplayName("Method removeKey / retainKey / replaceKey test")
	class removeRetainReplaceKeyTest {
		@Test
		void testRemoveKeyRecursive() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", new HashMap<>(Map.of("b", 1, "c", 2)));
			map.put("l", new ArrayList<>(List.of(new HashMap<>(Map.of("b", 3)))));
			MapUtil.removeKey(map, "a.b");
			assertFalse(((Map<?, ?>) map.get("a")).containsKey("b"));
			MapUtil.removeKey(map, "l.b");
			assertFalse(((Map<?, ?>) ((List<?>) map.get("l")).get(0)).containsKey("b"));
		}

		@Test
		void testRetainKeyRecursive() {
			Map<String, Object> oldMap = new HashMap<>();
			oldMap.put("a", new HashMap<>(Map.of("b", 1, "c", 2)));
			oldMap.put("l", new ArrayList<>(List.of(new HashMap<>(Map.of("b", 3, "d", 4)))));
			Map<String, Object> newMap = new HashMap<>();
			MapUtil.retainKey(newMap, oldMap, "a.b");
			MapUtil.retainKey(newMap, oldMap, "l.b");
			assertEquals(1, ((Number) ((Map<?, ?>) newMap.get("a")).get("b")).intValue());
			assertEquals(3, ((Map<?, ?>) ((List<?>) newMap.get("l")).get(0)).get("b"));
		}

		@Test
		void testReplaceKeyRecursive() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", new HashMap<>(Map.of("b", 1)));
			map.put("l", new ArrayList<>(List.of(new HashMap<>(Map.of("b", 2)))));
			MapUtil.replaceKey("a.b", map, "bb");
			assertEquals(1, ((Map<?, ?>) map.get("a")).get("bb"));
			assertFalse(((Map<?, ?>) map.get("a")).containsKey("b"));
			MapUtil.replaceKey("l.b", map, "bb");
			assertEquals(2, ((Map<?, ?>) ((List<?>) map.get("l")).get(0)).get("bb"));
		}
	}

	@Nested
	@DisplayName("Method recursiveRemoveKey / recursiveRemoveByValuePredicate / removeNullValue test")
	class recursiveRemoveTest {
		@Test
		void testRecursiveRemoveKey() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", 1);
			map.put("b", new HashMap<>(Map.of("a", 2)));
			map.put("l", new ArrayList<>(List.of(new HashMap<>(Map.of("a", 3)))));
			MapUtil.recursiveRemoveKey(map, "a");
			assertFalse(map.containsKey("a"));
			assertFalse(((Map<?, ?>) map.get("b")).containsKey("a"));
			assertFalse(((Map<?, ?>) ((List<?>) map.get("l")).get(0)).containsKey("a"));
		}

		@Test
		void testRecursiveRemoveByPredicate() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", "x");
			map.put("b", new HashMap<>(Map.of("c", "x")));
			map.put("l", new ArrayList<>(List.of("x", new HashMap<>(Map.of("d", "x")))));
			MapUtil.recursiveRemoveByValuePredicate(map, o -> "x".equals(o));
			assertFalse(map.containsKey("a"));
			assertTrue(((Map<?, ?>) map.get("b")).isEmpty());
			assertEquals(1, ((List<?>) map.get("l")).size());
		}

		@Test
		void testRemoveNullValueAndNaN() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", null);
			map.put("b", Double.NaN);
			Map<String, Object> c = new HashMap<>();
			c.put("d", null);
			c.put("e", 1);
			map.put("c", c);
			List<Object> l = new ArrayList<>();
			l.add(null);
			l.add(Double.NaN);
			l.add(2);
			map.put("l", l);
			MapUtil.removeNullValue(map);
			assertFalse(map.containsKey("a"));
			assertFalse(map.containsKey("b"));
			assertFalse(((Map<?, ?>) map.get("c")).containsKey("d"));
			assertEquals(1, ((Map<?, ?>) map.get("c")).get("e"));
			assertEquals(List.of(2), map.get("l"));
		}
	}

	@Nested
	@DisplayName("Method keyToLowerCase / keyToUpperCase / getAllKeys test")
	class keyAndKeysTest {
		@Test
		void testKeyCaseConvert() {
			Map<String, Object> map = new HashMap<>();
			map.put("A", new HashMap<>(Map.of("B", 1)));
			map.put("L", new ArrayList<>(List.of(new HashMap<>(Map.of("C", 2)))));
			map.put("N", null);

			Map<String, Object> lower = MapUtil.keyToLowerCase(map);
			assertTrue(lower.containsKey("a"));
			assertTrue(((Map<?, ?>) lower.get("a")).containsKey("b"));
			assertEquals(2, ((Map<?, ?>) ((List<?>) lower.get("l")).get(0)).get("c"));
			assertTrue(lower.containsKey("n"));

			Map<String, Object> upper = MapUtil.keyToUpperCase(lower);
			assertTrue(upper.containsKey("A"));
			assertTrue(((Map<?, ?>) upper.get("A")).containsKey("B"));
			assertEquals(2, ((Map<?, ?>) ((List<?>) upper.get("L")).get(0)).get("C"));
		}

		@Test
		void testKeyToLowerCaseLayerLimit() {
			Map<String, Object> map = new HashMap<>();
			map.put("A", new HashMap<>(Map.of("B", new HashMap<>(Map.of("C", 1)))));
			Map<String, Object> lower1 = MapUtil.keyToLowerCase(map, 1, 1);
			assertTrue(lower1.containsKey("a"));
			assertTrue(((Map<?, ?>) lower1.get("a")).containsKey("B"));
		}

		@Test
		void testGetAllKeys() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", new HashMap<>(Map.of("b", 1)));
			map.put("l", new ArrayList<>(List.of(new HashMap<>(Map.of("c", 2)))));
			Set<String> flat = MapUtil.getAllKeys(map, false);
			assertEquals(new HashSet<>(Set.of("a", "l")), new HashSet<>(flat));
			Set<String> all = MapUtil.getAllKeys(map, true);
			assertTrue(all.containsAll(Set.of("a", "a.b", "l", "l.c")));
		}
	}

	@Nested
	@DisplayName("Method recursiveMap / iterate test")
	class recursiveAndIterateTest {
		@Test
		void testRecursiveMapHandler() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", new HashMap<>(Map.of("b", 1)));
			map.put("l", new ArrayList<>(List.of(new HashMap<>(Map.of("c", 2)))));
			Map<String, Object> newMap = MapUtil.recursiveMap(map, (k, v, p) -> {
				String fullKey = StringUtils.isNotBlank(p) ? p + "." + k : k;
				return new MapUtil.MapEntry(fullKey, v);
			});
			assertEquals(1, ((Map<?, ?>) map.get("a")).get("a.b"));
			assertEquals(2, ((Map<?, ?>) ((List<?>) map.get("l")).get(0)).get("l.c"));
			assertTrue(newMap.containsKey("a"));
			assertTrue(((Map<?, ?>) newMap.get("a")).containsKey("a.b"));
			assertTrue(newMap.containsKey("l"));
			assertTrue(((Map<?, ?>) ((List<?>) newMap.get("l")).get(0)).containsKey("l.c"));
		}

		@Test
		void testIterateRenameLeafKeys() {
			Map<String, Object> map = new HashMap<>();
			map.put("a", 1);
			map.put("m", new HashMap<>(Map.of("b", 2)));
			map.put("l", new ArrayList<>(List.of(new HashMap<>(Map.of("c", 3)))));
			MapUtil.iterate(map, String::toUpperCase);
			assertFalse(map.containsKey("a"));
			assertEquals(1, map.get("A"));
			assertTrue(((Map<?, ?>) map.get("m")).containsKey("B"));
			assertTrue(((Map<?, ?>) ((List<?>) map.get("l")).get(0)).containsKey("C"));
		}
	}
}
