package com.tapdata.constant;

import com.tapdata.entity.RelateDatabaseField;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class MapUtilTest {

	private Map<String, Object> map;

	@Before
	public void init() {
		map = new HashMap<>();

		map.put("a", new HashMap<String, Object>() {{
			put("b", new HashMap<String, Object>() {{
				put("c", 1);
			}});
			put("d", "test");
		}});
		map.put("name", "sam");
		map.put("a_b_", "test");
		map.put("a_b_d", "test1");
		map.put("e.f", "test2");
	}

	@Test
	public void getValueByKeyTest() {
		assertEquals(1, MapUtil.getValueByKey(map, "a.b.c", "_"));
		assertEquals("sam", MapUtil.getValueByKey(map, "name", "_"));
		assertEquals("test", MapUtil.getValueByKey(map, "a.b.", "_"));
		assertEquals("test1", MapUtil.getValueByKey(map, "a.b.d", "_"));
		assertEquals("test2", MapUtil.getValueByKey(map, "e.f"));
	}

	@Test
	public void removeValueByKeyTest() {
		assertTrue(MapUtil.removeValueByKey(map, "a.b"));
		assertFalse(((Map) map.get("a")).containsKey("b"));
		assertTrue(MapUtil.removeValueByKey(map, "name"));
		assertFalse(map.containsKey("name"));
		assertTrue(MapUtil.removeValueByKey(map, "a.b.", "_"));
		assertFalse(map.containsKey("a_b_"));
	}

	@Test
	public void getValuePositionInMapTest() {
		assertEquals(1, MapUtil.getValuePositionInMap(map, "name"));
		assertEquals(0, MapUtil.getValuePositionInMap(map, "not_exists_key"));
		assertTrue(MapUtil.getValuePositionInMap(map, "a.b") > 1);
	}

	@Test
	public void putValueInMapTest() throws Exception {
		final int size = map.size();
		MapUtil.putValueInMap(map, "test", "test3");
		assertEquals(size + 1, map.size());

		MapUtil.putValueInMap(map, "sub.name", "sam");
		assertTrue(map.containsKey("sub"));
		final Object sub = map.get("sub");
		assertTrue(sub instanceof Map);
		assertTrue(((Map) sub).containsKey("name"));
		assertEquals("sam", ((Map) sub).get("name"));
	}

	@Test
	public void recursiveMapWhenLoadSchemaTest() throws IOException {
		String jsonStr = "{\"id\": 1, \"sub_doc\": {\"sub1\": \"value1\", \"sub2\": null, \"sub3\": {\"sub4\":1, \"sub5\": {\"sub6\": \"sub6_value\"}}}, \"arr\": [\"value2\", {\"arr1\": 1, \"arr2\": \"value3\"}, 3], \"status\": 0}";

		final Map<String, Object> map = JSONUtil.json2Map(jsonStr);

		List<RelateDatabaseField> fields = new ArrayList<>();
		MapUtil.recursiveMapWhenLoadSchema(fields, map, "test", "");

		assertEquals(12, fields.size());
	}

}
