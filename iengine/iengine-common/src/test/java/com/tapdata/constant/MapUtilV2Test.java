package com.tapdata.constant;

import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tapdata.constant.MapUtilV2.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-07-23 17:40
 **/
@DisplayName("Class MapUtilV2 Test")
class MapUtilV2Test {

	private Map<String, Object> map;

	@BeforeEach
	void setUp() {
		map = new HashMap<>();
		map.put("key1", "key1-value");
		map.put("key2", new HashMap<String, Object>() {{
			put("key3", "key2-key3-value");
		}});
		map.put("key4", new ArrayList<Object>() {{
			add(1);
			add(new HashMap<String, Object>() {{
				put("key5", "key4-key5-value");
			}});
			add(new HashMap<String, Object>() {{
				put("key5", "key4-key5-value1");
			}});
		}});
		map.put("key6. ", "key6-value");
		map.put("key7.key8", "key7-key8-value");
		map.put("key9", new TapMapValue(new HashMap<String, Object>() {{
			put("key10", "key9-key10-value");
		}}));
		map.put("key11", new TapArrayValue(new ArrayList<Object>() {{
			add(new HashMap<String, Object>() {{
				put("key12", "key11-key12-value");
			}});
		}}));
	}

	@Test
	@DisplayName("Method getValueByKey test")
	void test1() {
		assertEquals("key1-value", getValueByKey(map, "key1"));
		assertEquals("key2-key3-value", getValueByKey(map, "key2.key3"));
		assertInstanceOf(NotExistsNode.class, getValueByKey(map, "key2.key4"));
		Object key4_key5 = getValueByKey(map, "key4.key5");
		assertInstanceOf(TapList.class, key4_key5);
		assertEquals(2, ((TapList) key4_key5).size());
		assertEquals("key4-key5-value", ((TapList) key4_key5).get(0));
		assertEquals("key4-key5-value1", ((TapList) key4_key5).get(1));
		assertInstanceOf(NotExistsNode.class, getValueByKey(map, "xxx"));
		assertNull(getValueByKey(map, ""));
		assertInstanceOf(NotExistsNode.class, getValueByKey(map, " . "));
		assertNull(getValueByKey(null, "xx"));
		assertNull(getValueByKey(new HashMap<>(), "xx"));
		assertInstanceOf(NotExistsNode.class, getValueByKey(map, "key6. "));
		assertInstanceOf(NotExistsNode.class, getValueByKey(map, "key7.key8"));
		assertEquals("key9-key10-value", getValueByKey(map, "key9.key10"));
		assertInstanceOf(NotExistsNode.class, getValueByKey(map, "key9.key11"));
		Object value = getValueByKey(map, "key11.key12");
		assertInstanceOf(TapList.class, value);
		assertEquals("key11-key12-value", ((TapList) value).get(0));
	}

	@Test
	@DisplayName("Method getValueByKeyV2 test")
	void test2() {
		Map<String, Object> map = new HashMap<>();
		assertNull(getValueByKeyV2(map, "xxx"));
	}

	@Test
	@DisplayName("Method removeValueByKey test")
	void test3() {
		assertTrue(removeValueByKey(map, "key1"));
		assertFalse(removeValueByKey(map, "xx"));
		assertTrue(removeValueByKey(map, "key2.key3"));
		assertTrue(removeValueByKey(map, "key4.key5"));
		assertTrue(removeValueByKey(map, "key6. "));
		assertTrue(removeValueByKey(map, "key7.key8"));
		assertTrue(removeValueByKey(map, "key9.key10"));
		assertTrue(removeValueByKey(map, "key11.key12"));
		assertFalse(removeValueByKey(null, "xx"));
		assertFalse(removeValueByKey(map, ""));
	}

	@Test
	@DisplayName("Method getValuePositionInMap test")
	void test4() {
		Map<String, Object> m = new HashMap<>();
		m.put("a", new HashMap<String, Object>() {{
			put("b", new HashMap<String, Object>() {{
				put("c", 1);
			}});
		}});
		assertEquals(3, MapUtilV2.getValuePositionInMap(m, "a.b.c", 0));
		assertEquals(2, MapUtilV2.getValuePositionInMap(m, "a.b.x", 0));
		assertEquals(1, MapUtilV2.getValuePositionInMap(m, "a", 0));
		assertEquals(0, MapUtilV2.getValuePositionInMap(new HashMap<>(), "a", 0));
	}

	@Test
	@DisplayName("Method putValueInMap merge behaviors")
	void test5() throws Exception {
		Map<String, Object> m = new HashMap<>();
		MapUtilV2.putValueInMap(m, "a.b", "\\.", new HashMap<>(Map.of("x", 1)));
		assertEquals(1, ((Map<?, ?>) ((Map<?, ?>) m.get("a")).get("b")).get("x"));

		MapUtilV2.putValueInMap(m, "a.b", "\\.", new HashMap<>(Map.of("y", 2)));
		Map ab = (Map) ((Map) m.get("a")).get("b");
		assertEquals(2, ab.get("y"));
		assertEquals(1, ab.get("x"));

		Map<String, Object> root = new HashMap<>();
		root.put("a", new HashMap<>(Map.of("l", new ArrayList<>(List.of(1, 2)))));
		MapUtilV2.putValueInMap(root, "a.l", "\\.", List.of(3, 4));
		assertEquals(List.of(1, 2, 3, 4), ((Map<?, ?>) root.get("a")).get("l"));
		MapUtilV2.putValueInMap(root, "a.l", "\\.", 5);
		assertEquals(List.of(1, 2, 3, 4, 5), ((Map<?, ?>) root.get("a")).get("l"));
	}

	@Test
	@DisplayName("Method putValueInMap conflict cases")
	void test6() {
		Map<String, Object> m = new HashMap<>();
		m.put("a", new HashMap<>(Map.of("b", 1)));
		assertThrows(Exception.class, () -> MapUtilV2.putValueInMap(m, "a.b", "\\.", new HashMap<>(Map.of("x", 1))));
	}

	@Test
	@DisplayName("Method putValueInMap with TapList into nested array")
	void test7() throws Exception {
		Map<String, Object> m = new HashMap<>();
		m.put("arr", new ArrayList<>(List.of(new HashMap<>(Map.of("b", "v1")), new HashMap<>(Map.of("b", "v2")))));
		TapList tl = new TapList();
		tl.add(0, "x1");
		tl.add(1, "x2");
		MapUtilV2.putValueInMap(m, "arr.b", "\\.", tl);
		assertEquals("x1", ((Map<?, ?>) ((List<?>) m.get("arr")).get(0)).get("b"));
		assertEquals("x2", ((Map<?, ?>) ((List<?>) m.get("arr")).get(1)).get("b"));
	}

	@Test
	@DisplayName("Method removeEmptyMap and containsKey")
	void test8() {
		Map<String, Object> m = new HashMap<>();
		m.put("a", new HashMap<>());
		m.put("b", new HashMap<>(Map.of("c", 1)));
		MapUtilV2.removeEmptyMap(m, "a");
		assertFalse(m.containsKey("a"));
		assertTrue(MapUtilV2.containsKey(m, "b.c"));
		assertTrue(MapUtilV2.containsKey(m, "b.x"));
	}
}
