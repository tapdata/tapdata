package com.tapdata.constant;

import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
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
		assertEquals("key4-key5-value", ((Map<?, ?>) ((TapList) key4_key5).get(0)).get("value"));
		assertEquals("key4-key5-value1", ((Map<?, ?>) ((TapList) key4_key5).get(1)).get("value"));
		assertInstanceOf(NotExistsNode.class, getValueByKey(map, "xxx"));
		assertNull(getValueByKey(map, ""));
		assertNull(getValueByKey(map, " . "));
		assertNull(getValueByKey(null, "xx"));
		assertNull(getValueByKey(new HashMap<>(), "xx"));
		assertEquals("key6-value", getValueByKey(map, "key6. "));
		assertEquals("key7-key8-value", getValueByKey(map, "key7.key8"));
		assertEquals("key9-key10-value", getValueByKey(map, "key9.key10"));
		assertInstanceOf(NotExistsNode.class, getValueByKey(map, "key9.key11"));
		Object value = getValueByKey(map, "key11.key12");
		assertInstanceOf(TapList.class, value);
		assertEquals("key11-key12-value", ((Map<?, ?>) ((TapList) value).get(0)).get("value"));
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
}