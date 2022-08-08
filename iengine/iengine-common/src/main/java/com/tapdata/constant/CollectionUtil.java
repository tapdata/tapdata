package com.tapdata.constant;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author samuel
 * @Description
 * @create 2020-12-10 15:03
 **/
public class CollectionUtil extends CollectionUtils {

	public static List getValueByKey(List list, String key) {
		TapList retList = new TapList();
		for (int i = 0; i < list.size(); i++) {
			Object o = list.get(i);
			if (o instanceof Map) {
				Object tempValue = MapUtilV2.getValueByKey((Map) o, key);
				if (tempValue instanceof NotExistsNode) {
					continue;
				}
				retList.add(i, tempValue);
			} else if (o instanceof List) {
				List tempValue = getValueByKey((List) o, key);
				if (CollectionUtils.isNotEmpty(tempValue)) {
					retList.add(i, tempValue);
				}
			}
		}

		return CollectionUtils.isNotEmpty(retList) ? retList : null;
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
