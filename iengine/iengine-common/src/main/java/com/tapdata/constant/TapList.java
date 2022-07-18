package com.tapdata.constant;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description Map获取的内嵌数组模型
 * <p>
 * [
 * {
 * index: 0, // 值在数组的下标
 * value: "value1" // 具体的值，可以是任意类型
 * },
 * {
 * index: 2,
 * value: "value2"
 * }
 * ]
 * <p>
 * 对于多层内嵌数组，value则会又是一个TapList
 * [
 * {
 * index: 0,
 * value: [
 * {
 * index: 3,
 * value: ""
 * }
 * ]
 * }
 * ]
 * <p>
 * 处理时，需要递归，直到value不是一个TapList，才是真实的值
 * @create 2020-12-10 18:35
 **/
public class TapList extends ArrayList {

	public final static String INDEX = "index";
	public final static String VALUE = "value";

	@Override
	public void add(int index, Object value) {
		if (index < 0) {
			return;
		}
		Map<String, Object> tempMap = new HashMap<>();
		tempMap.put("index", index);
		tempMap.put("value", value);

		super.add(tempMap);
	}

	public int getIndex(int index) throws Exception {
		Object o = super.get(index);
		if (o instanceof Map && ((Map) o).containsKey(INDEX)) {
			try {
				return Integer.valueOf(((Map) o).get(INDEX).toString());
			} catch (Exception e) {
				throw e;
			}
		} else {
			return 0;
		}
	}

	public Object getValue(int index) {
		Object o = super.get(index);
		if (o instanceof Map && ((Map) o).containsKey(VALUE)) {
			return ((Map) o).get(VALUE);
		} else {
			return null;
		}
	}

	public void setValue(int index, Object value) {
		Object o = super.get(index);
		if (o instanceof Map && ((Map) o).containsKey(VALUE)) {
			((Map) o).put(VALUE, value);
		}
	}
}
