package com.tapdata.constant;

import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Class CollectionUtil Test")
class CollectionUtilTest {
	@Test
	@DisplayName("Method getValueByKey should return empty list for null/empty input")
	void testGetValueByKey_EmptyInput() {
		assertTrue(CollectionUtil.getValueByKey(null, "a").isEmpty());
		assertTrue(CollectionUtil.getValueByKey(new ArrayList<>(), "a").isEmpty());
		assertTrue(CollectionUtil.getValueByKey(Arrays.asList(Map.of("a", 1)), null).isEmpty());
		assertTrue(CollectionUtil.getValueByKey(Arrays.asList(Map.of("a", 1)), "").isEmpty());
	}

	@Test
	@DisplayName("Method getValueByKey should collect non-empty values and keep nested TapList")
	void testGetValueByKey_CollectValues() {
		Map<String, Object> map1 = new HashMap<>();
		map1.put("a", 1);

		Map<String, Object> map2 = new HashMap<>();
		map2.put("a", null);

		Map<String, Object> map3 = new HashMap<>();
		map3.put("b", 2);

		Map<String, Object> map4 = new HashMap<>();
		map4.put("a", Collections.emptyList());

		List<Object> nestedList = Arrays.asList(
				new HashMap<>(Map.of("a", 5)),
				new HashMap<>(Map.of("a", 6))
		);

		List<Object> tapArray = Arrays.asList(
				new HashMap<>(Map.of("a", 8)),
				new HashMap<>(Map.of("a", 9))
		);

		List<Object> list = Arrays.asList(
				map1,
				map2,
				map3,
				map4,
				nestedList,
				new TapMapValue(new HashMap<>(Map.of("a", 7))),
				new TapArrayValue(tapArray)
		);

		List<Object> values = (List<Object>) CollectionUtil.getValueByKey(list, "a");
		assertInstanceOf(TapList.class, values);
		assertEquals(4, values.size());
		assertEquals(1, values.get(0));

		assertInstanceOf(TapList.class, values.get(1));
		TapList nested = (TapList) values.get(1);
		assertEquals(2, nested.size());
		assertEquals(5, nested.get(0));
		assertEquals(6, nested.get(1));

		assertEquals(7, values.get(2));

		assertInstanceOf(TapList.class, values.get(3));
		TapList nested2 = (TapList) values.get(3);
		assertEquals(Arrays.asList(8, 9), nested2);
	}

	@Test
	@DisplayName("Method parseValue should parse Map/TapMapValue/List/TapArrayValue and return null for others")
	void testParseValue() {
		Map<String, Object> map = new HashMap<>();
		map.put("a", 1);
		assertEquals(1, CollectionUtil.parseValue(map, "a"));
		assertInstanceOf(NotExistsNode.class, CollectionUtil.parseValue(map, "missing"));
		assertEquals(2, CollectionUtil.parseValue(new TapMapValue(new HashMap<>(Map.of("a", 2))), "a"));
		assertNull(CollectionUtil.parseValue("not-supported", "a"));

		List<Object> list = Arrays.asList(new HashMap<>(Map.of("a", 3)));
		Object value = CollectionUtil.parseValue(list, "a");
		assertInstanceOf(TapList.class, value);
		assertEquals(3, ((TapList) value).get(0));

		Object value2 = CollectionUtil.parseValue(new TapArrayValue(list), "a");
		assertInstanceOf(TapList.class, value2);
		assertEquals(3, ((TapList) value2).get(0));
	}

	@Test
	@DisplayName("Method putTapListInList should put values by index and handle nested TapList")
	void testPutTapListInList() throws Exception {
		List<Object> list = new ArrayList<>();
		TapList tapList = new TapList();
		tapList.add(0, "v1");
		CollectionUtil.putTapListInList(list, "k", tapList);
		assertEquals(1, list.size());
		assertEquals("v1", ((Map<?, ?>) list.get(0)).get("k"));

		TapList tapList2 = new TapList();
		tapList2.add(0, "v2");
		CollectionUtil.putTapListInList(list, "k", tapList2);
		assertEquals(1, list.size());
		assertEquals("v2", ((Map<?, ?>) list.get(0)).get("k"));

		List<Object> listWithNull = new ArrayList<>();
		listWithNull.add(null);
		TapList tapList3 = new TapList();
		tapList3.add(0, "v3");
		CollectionUtil.putTapListInList(listWithNull, "k", tapList3);
		assertEquals(2, listWithNull.size());
		assertEquals("v3", ((Map<?, ?>) listWithNull.get(0)).get("k"));
		assertNull(listWithNull.get(1));

		List<Object> nestedList = new ArrayList<>();
		List<Object> inner = new ArrayList<>();
		nestedList.add(inner);
		TapList nestedTapList = new TapList();
		TapList innerTapList = new TapList();
		innerTapList.add(0, "x");
		nestedTapList.add(0, innerTapList);
		CollectionUtil.putTapListInList(nestedList, "k", nestedTapList);
		assertEquals(1, inner.size());
		assertEquals("x", ((Map<?, ?>) inner.get(0)).get("k"));

		List<Object> mapHolder = new ArrayList<>();
		Map<String, Object> holderMap = new HashMap<>();
		mapHolder.add(holderMap);
		TapList nestedTapList2 = new TapList();
		TapList innerTapList2 = new TapList();
		innerTapList2.add(0, "y");
		nestedTapList2.add(0, innerTapList2);
		CollectionUtil.putTapListInList(mapHolder, "k", nestedTapList2);
		assertInstanceOf(TapList.class, holderMap.get("k"));
		assertEquals("y", ((TapList) holderMap.get("k")).getValue(0));
	}

	@Test
	@DisplayName("Method removeKeyFromList should remove empty maps and return removed flag")
	void testRemoveKeyFromList() {
		List<Object> list = new ArrayList<>();
		list.add(new HashMap<>(Map.of("a", 1)));
		list.add(new HashMap<>(Map.of("b", 2)));
		list.add(null);
		list.add(new ArrayList<>(List.of(new HashMap<>(Map.of("a", 3)))));

		assertTrue(CollectionUtil.removeKeyFromList(list, "a"));
		assertEquals(3, list.size());
		assertEquals(Map.of("b", 2), list.get(0));
		assertNull(list.get(1));
		assertInstanceOf(List.class, list.get(2));
		assertTrue(((List<?>) list.get(2)).isEmpty());

		List<Object> list2 = new ArrayList<>(List.of(new HashMap<>(Map.of("b", 2))));
		assertFalse(CollectionUtil.removeKeyFromList(list2, "a"));
		assertEquals(1, list2.size());

		assertFalse(CollectionUtil.removeKeyFromList(new ArrayList<>(), "a"));
	}

	@Test
	@DisplayName("Method putInTapList should put value into Map/List values and nested TapList")
	void testPutInTapList() throws Exception {
		TapList tapList = new TapList();

		Map<String, Object> v0 = new HashMap<>();
		tapList.add(0, v0);

		Map<String, Object> v1a = new HashMap<>();
		Map<String, Object> v1b = new HashMap<>(Map.of("x", 1));
		tapList.add(1, Arrays.asList(v1a, null, v1b));

		TapList nested = new TapList();
		Map<String, Object> v2 = new HashMap<>();
		nested.add(0, v2);
		tapList.add(nested);

		CollectionUtil.putInTapList(tapList, "k", "v");

		assertEquals("v", ((Map<?, ?>) tapList.getValue(0)).get("k"));
		assertEquals("v", v1a.get("k"));
		assertEquals("v", v1b.get("k"));
		assertEquals("v", ((Map<?, ?>) nested.getValue(0)).get("k"));
	}

	@Test
	@DisplayName("Method tapListValueInvokeFunction should apply function on VALUE for all leaf nodes")
	void testTapListValueInvokeFunction() {
		TapList tapList = new TapList();
		tapList.add(0, 1);

		TapList nested = new TapList();
		nested.add(0, 2);
		tapList.add(1, nested);
		tapList.add(2, "s");

		CollectionUtil.tapListValueInvokeFunction(tapList, v -> v == null ? null : v.toString() + "!");

		assertEquals("1!", tapList.getValue(0));
		assertEquals("2!", nested.getValue(0));
		assertEquals("s!", tapList.getValue(2));

		TapList tapList2 = new TapList();
		tapList2.add(0, 1);
		CollectionUtil.tapListValueInvokeFunction(tapList2, null);
		assertEquals(1, tapList2.getValue(0));
	}

	@Test
	@DisplayName("Method putInTapList should no-op for empty TapList")
	void testPutInTapList_WhenTapListEmpty_ShouldReturn() throws Exception {
		TapList empty = new TapList();
		CollectionUtil.putInTapList(empty, "k", "v");
		assertTrue(empty.isEmpty());
	}
}
