package com.tapdata.constant;

import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.*;
import java.util.function.Function;

/**
 * @author samuel
 * @Description
 * @create 2020-12-10 15:03
 **/
public class CollectionUtil  {

	private CollectionUtil() {}

	public static <T>List<T> getValueByKey(List<T> list, String key) {
		if (list == null || list.isEmpty() || key == null || key.isEmpty()) {
			return new ArrayList<>();
		}
		TapList result = null;
        for (Object o : list) {
            Object value = parseValue(o, key);
            if (value == null
                    || value instanceof NotExistsNode
                    || (value instanceof List && ((List<?>) value).isEmpty())) {
                continue;
            }
            if (result == null) {
                result = new TapList();
            }
            result.add(value);
        }
		return result;
	}

	static Object parseValue(Object o, String key) {
		if (o instanceof Map) {
			return MapUtilV2.getValueByKey((Map<String, Object>) o, key);
		}
		if (o instanceof TapMapValue v) {
			return MapUtilV2.getValueByKey(v.getValue(), key);
		}
		if (o instanceof List) {
			return getValueByKey((List<?>) o, key);
		}
		if (o instanceof TapArrayValue v) {
			return getValueByKey(v.getValue(), key);
		}
		return null;
	}

	public static void putTapListInList(List list, String key, TapList tapList) throws Exception {
		for (int i = 0; i < tapList.size(); i++) {
			int index = tapList.getIndex(i);
			Object tapValue = tapList.getValue(i);

			if (index < 0) {
				continue;
			}

			if (tapValue instanceof TapList) {
				Object o = list.get(index);
				if (o instanceof List) {
					putTapListInList((List) o, key, (TapList) tapValue);
				} else if (o instanceof Map) {
					MapUtilV2.putValueInMap((Map) o, key, tapValue);
				}
			} else {
				if (list.size() - 1 < index) {

					Map<String, Object> map = new HashMap<>();
					map.put(key, tapValue);
					list.add(map);

				} else {
					Object o = list.get(index);

					if (o instanceof Map) {
						((Map) o).put(key, tapValue);
					} else if (o == null) {
						o = new HashMap<>();
						((Map) o).put(key, tapValue);
						list.add(index, o);
					}
				}
			}
		}
	}

	/**
	 * 从对象数组移除元素，如果某个对象为空，则也会移除该对象
	 *
	 * @param list
	 * @param key
	 * @return
	 */
	public static boolean removeKeyFromList(List list, String key) {
		if (CollectionUtils.isEmpty(list)) {
			return false;
		}

		boolean isRemoved = false;

		Iterator iterator = list.iterator();
		while (iterator.hasNext()) {
			Object node = iterator.next();

			if (node == null) {
				continue;
			}

			if (node instanceof Map) {
				// 递归调用Map的移除方法
				// 如果List中有被删除的，isRemoved字段的值保持为true
				if (!isRemoved) {
					isRemoved = MapUtilV2.removeValueByKey((Map) node, key);
				} else {
					// 递归调用List的移除方法
					MapUtilV2.removeValueByKey((Map) node, key);
				}

				if (MapUtils.isEmpty((Map) node)) {
					iterator.remove();
				}
			} else if (node instanceof List) {
				isRemoved = removeKeyFromList((List) node, key);
			}
		}

		return isRemoved;
	}

	public static void putInTapList(TapList tapList, String key, Object value) throws Exception {
		if (CollectionUtils.isEmpty(tapList)) {
			return;
		}

		for (int i = 0; i < tapList.size(); i++) {
			Object node = tapList.get(i);
			if (node == null) {
				continue;
			}

			if (node instanceof TapList) {
				putInTapList((TapList) node, key, value);
			} else if (node instanceof Map) {
				Object tapValue = tapList.getValue(i);
				if (tapValue instanceof Map) {
					MapUtilV2.putValueInMap((Map) tapValue, key, value);
				} else if (tapValue instanceof List) {
					for (int j = 0; j < ((List) tapValue).size(); j++) {
						Object o = ((List) tapValue).get(j);

						if (o == null) {
							continue;
						}

						if (o instanceof Map) {
							MapUtilV2.putValueInMap((Map) o, key, value);
						}
					}
				}
			}
		}
	}

	public static void tapListValueInvokeFunction(TapList tapList, Function function) {
		if (CollectionUtils.isEmpty(tapList) || function == null) {
			return;
		}

		for (int i = 0; i < tapList.size(); i++) {
			Object value = tapList.getValue(i);
			if (value instanceof TapList) {
				tapListValueInvokeFunction((TapList) value, function);
			} else {
				Object node = tapList.get(i);

				if (node instanceof Map) {
					Object nodeValue = ((Map) node).get(TapList.VALUE);

					((Map) node).put(TapList.VALUE, function.apply(nodeValue));
				}

			}
		}
	}
}
