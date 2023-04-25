package com.tapdata.constant;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2020-12-10 17:09
 **/
public class MapUtilV2 extends MapUtil {

	private static NotExistsNode notExistsNode = new NotExistsNode();

	/**
	 * 支持从内嵌数组取值，如果是内嵌数组，返回值会是一个集合
	 * [{index=2, value='value'}] index表示在数组中的下表，value表示具体的值
	 *
	 * @param dataMap
	 * @param key
	 * @return 注意：使用时,如果不存在会返回{@link NotExistsNode},这个类用于区分不存在还是Null
	 * @throws NullPointerException
	 */
	public static Object getValueByKey(Map<String, Object> dataMap, String key) throws NullPointerException {
		Object value;

		if (MapUtils.isEmpty(dataMap) || StringUtils.isBlank(key)) {
			return null;
		}

		if (needSplit(key)) {
			/* 多层级获取值，利用递归逐层获取 */

			String[] split = key.split("\\.");

			if (split.length <= 0 || StringUtils.isAllBlank(split)) {
				return null;
			}

			List<String> keys = Arrays.stream(split).filter(s -> StringUtils.isNotBlank(s)).collect(Collectors.toList());

			if (keys.size() <= 1) {
				return dataMap.getOrDefault(key, notExistsNode);
			} else {
				value = dataMap.getOrDefault(keys.get(0), notExistsNode);

				if (value == null) {
					return dataMap.getOrDefault(key, notExistsNode);
				}

				// 截掉第一层字段，例如：a.b.c -> b.c，用于递归
				String recursiveKey = keys.subList(1, keys.size()).stream().collect(Collectors.joining("."));

				// 递归处理Map或者List
				if (value instanceof Map) {
					value = getValueByKey((Map) value, recursiveKey);
					if (value instanceof NotExistsNode) {
						value = dataMap.getOrDefault(key, notExistsNode);
					}
				} else if (value instanceof List) {
					value = CollectionUtil.getValueByKey((List) value, recursiveKey);
				}
			}
		} else {
			/* 单层获取值 */
			value = dataMap.getOrDefault(key, notExistsNode);
		}

		return value;
	}

	/**
	 * 自动处理{@link NotExistsNode}
	 *
	 * @param dataMap
	 * @param key
	 * @return
	 */
	public static Object getValueByKeyV2(Map<String, Object> dataMap, String key) {
		Object value = getValueByKey(dataMap, key);
		value = value instanceof NotExistsNode ? null : value;
		return value;
	}

	/**
	 * 获取字段在Map里面的层级数
	 *
	 * @param dataMap
	 * @param key
	 * @param position
	 * @return
	 */
	public static int getValuePositionInMap(Map<String, Object> dataMap, String key, int position) {
		Object value;

		if (MapUtils.isEmpty(dataMap) || StringUtils.isBlank(key)) {
			return 0;
		}

		if (needSplit(key)) {
			String[] split = key.split("\\.");

			if (split.length <= 0 || StringUtils.isAllBlank(split)) {
				return 0;
			}

			List<String> keys = Arrays.stream(split).filter(s -> StringUtils.isNotBlank(s)).collect(Collectors.toList());

			if (keys.size() <= 1) {
				position = 1;
			} else {
				value = dataMap.getOrDefault(keys.get(0), null);

				if (value == null) {
					position = 0;
				}

				String recursiveKey = keys.subList(1, keys.size()).stream().collect(Collectors.joining("."));

				if (value instanceof Map) {
					position = getValuePositionInMap((Map) value, recursiveKey, ++position);
				} else if (value instanceof List) {
					int tempPosition;
					for (int j = 0; j < ((List<?>) value).size(); j++) {
						Object o = ((List<?>) value).get(j);
						if (o instanceof Map) {
							tempPosition = getValuePositionInMap((Map<String, Object>) o, recursiveKey, ++position);
							if (tempPosition > 0) {
								position = tempPosition;
								break;
							}
						}
					}
				}
			}
		} else {
			if (dataMap.containsKey(key)) {
				position++;
			}
		}

		return position;
	}

	/**
	 * 将键值对放入Map
	 * <p>
	 * 如果是放入内嵌数组，需先调用方法{@link MapUtilV2#getValueByKey}获取到{@link TapList}
	 * 按照需要修改TapList后，作为value传入该函数，即可放回内嵌数组的原位
	 *
	 * @param dataMap 需要写入的Map
	 * @param key     多层级使用"."作为分隔符
	 * @param value   需要写入的值
	 * @throws Exception
	 */
	public static void putValueInMap(Map<String, Object> dataMap, String key, Object value) throws Exception {
		putValueInMap(dataMap, key, ".", value);
	}

	/**
	 * 将键值对放入Map
	 * <p>
	 * 如果是放入内嵌数组，需先调用方法{@link MapUtilV2#getValueByKey}获取到{@link TapList}
	 * 按照需要修改TapList后，作为value传入该函数，即可放回内嵌数组的原位
	 *
	 * @param dataMap  需要写入的Map
	 * @param key      map的key
	 * @param splitStr key分隔符，空则表示不分层
	 * @param value    需要写入的值
	 * @throws Exception
	 */
	public static void putValueInMap(Map<String, Object> dataMap, String key, String splitStr, Object value) throws Exception {
		if (dataMap == null) {
			return;
		}
		String[] split;
		if (StringUtils.isNotBlank(splitStr)) {
			split = key.split(splitStr);
		} else {
			split = new String[]{key};
		}
		Object tempValue = dataMap;
		if (split.length <= 0 && StringUtils.isAllBlank(split)) {
			dataMap.put(key, value);
			return;
		}
		List<String> keys = Arrays.stream(split).filter(StringUtils::isNotBlank).collect(Collectors.toList());
		if (keys.size() <= 1) {
			putInMapWithMerge(value, tempValue, keys, 0, key);
		} else {
			if (value instanceof TapList) {
				putTapListInMap(dataMap, keys, (TapList) value);
			} else {
				for (int i = 0; i < keys.size(); i++) {
					String subKey = keys.get(i);
					if (tempValue instanceof Map) {
						if (i < keys.size() - 1) {
							if (!((Map) tempValue).containsKey(subKey)) {
								((Map) tempValue).put(subKey, new HashMap<>());
							}
							tempValue = ((Map) tempValue).get(subKey);
						} else {
							putInMapWithMerge(value, tempValue, keys, i, subKey);
						}
					} else if (tempValue instanceof List) {
						if (CollectionUtils.isEmpty((Collection) tempValue)) {
							((List) tempValue).add(new HashMap<>());
						}
						for (Object o : (List) tempValue) {
							putInMapWithMerge(value, o, keys, i, subKey);
						}
					}
				}
			}
		}
	}

	private static void putInMapWithMerge(Object value, Object tempValue, List<String> keys, int i, String subKey) throws Exception {
		if (!(((Map<?, ?>) tempValue).containsKey(subKey))) {
			((Map) tempValue).put(subKey, value);
		} else {
			if (((Map<?, ?>) tempValue).get(subKey) instanceof Map) {
				if (value instanceof Map) {
					((Map) ((Map<?, ?>) tempValue).get(subKey)).putAll((Map) value);
				} else {
					throw new Exception("Found key " + String.join(".", keys.subList(0, i + 1)) + " is a map: " + ((Map<?, ?>) tempValue).get(subKey) + ", value: " + value + " cannot put into it because missing key");
				}
			} else if (((Map<?, ?>) tempValue).get(subKey) instanceof Collection) {
				if (value instanceof Collection) {
					((Collection) ((Map<?, ?>) tempValue).get(subKey)).addAll((Collection) value);
				} else {
					((Collection) ((Map<?, ?>) tempValue).get(subKey)).add(value);
				}
			} else {
				throw new Exception("Key " + String.join(".", keys.subList(0, i + 1)) + " exists, value=" + ((Map<?, ?>) tempValue).get(subKey) + " is not a map or list, cannot overwrite it with new value: " + value);
			}
		}
	}

	/**
	 * 将TapList放入Map
	 * 作为方法{@link MapUtilV2#putValueInMap}的辅助方法，专门处理多层内嵌数组
	 *
	 * @param dataMap
	 * @param keys
	 * @param value
	 * @throws Exception
	 */
	private static void putTapListInMap(Map dataMap, List<String> keys, TapList value) throws Exception {

		if (MapUtils.isEmpty(dataMap)
				|| CollectionUtils.isEmpty(keys)
				|| keys.size() <= 1
				|| CollectionUtils.isEmpty(value)) {
			return;
		}

		Object parentValue = dataMap;
		for (int i = 0; i < keys.size(); i++) {
			String key = keys.get(i);
			if (parentValue instanceof Map) {
				parentValue = ((Map) parentValue).getOrDefault(key, null);
				if (parentValue == null) {
					return;
				}

				if (parentValue instanceof List) {
					String recursiveKey = keys.subList(i + 1, keys.size()).stream().collect(Collectors.joining("."));
					CollectionUtil.putTapListInList((List) parentValue, recursiveKey, value);
					break;
				}
			}
		}
	}

	/**
	 * 移除map中对应的key
	 * 对于内嵌数组，则会移除内嵌数组中所有符合key的元素
	 *
	 * @param dataMap
	 * @param key
	 * @return
	 */
	public static boolean removeValueByKey(Map<String, Object> dataMap, String key) {
		boolean isRemoved = false;
		if (MapUtils.isEmpty(dataMap) || StringUtils.isBlank(key)) {
			return false;
		}

		if (needSplit(key)) {
			String[] split = key.split("\\.");

			if (split.length <= 0 && StringUtils.isAllBlank(split)) {
				return false;
			}

			List<String> keys = Arrays.stream(split).filter(s -> StringUtils.isNotBlank(s)).collect(Collectors.toList());

			if (keys.size() <= 1) {
				if (dataMap.containsKey(key)) {
					dataMap.remove(key);
					isRemoved = true;
				}
			} else {
				Object value = dataMap.getOrDefault(keys.get(0), null);

				if (value == null) {
					if (dataMap.containsKey(key)) {
						dataMap.remove(key);
						isRemoved = true;
					}
					return isRemoved;
				}

				// 截取第一个，拼接成新的字段名，如a.b.c变为b.c，往里面递归
				String recursiveKey = keys.subList(1, keys.size()).stream().collect(Collectors.joining("."));

				if (value instanceof Map) {
					// 递归调用Map的移除方法
					isRemoved = removeValueByKey((Map) value, recursiveKey);
				} else if (value instanceof List) {
					// 递归调用List的移除方法
					isRemoved = CollectionUtil.removeKeyFromList((List) value, recursiveKey);
				}
			}

			removeEmptyMap(dataMap, keys.get(0));
		} else {
			if (dataMap.containsKey(key)) {
				dataMap.remove(key);
				isRemoved = true;
			}
		}

		return isRemoved;
	}

	/**
	 * 移除map里面的空map，支持第一层
	 * <p>
	 * 如果传入key，则只检查key对应的value是否为空map
	 * 如果key传入空值，则移除所有的空map
	 * <p>
	 * 例如，传入map：{key1: {}, key2: "value"}
	 * 返回：{key2: "value"}
	 *
	 * @param dataMap
	 * @param key
	 */
	public static void removeEmptyMap(Map dataMap, String key) {
		if (StringUtils.isNotBlank(key)) {
			if (dataMap.containsKey(key)
					&& dataMap.get(key) instanceof Map
					&& MapUtils.isEmpty((Map) dataMap.get(key))) {

				dataMap.remove(key);
			}
		} else {
			Iterator iterator = dataMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry entry = (Map.Entry) iterator.next();
				if (entry == null) {
					continue;
				}
				if (entry.getValue() instanceof Map && MapUtils.isEmpty((Map) entry.getValue())) {
					iterator.remove();
				}
			}
		}
	}

	public static boolean containsKey(Map<String, Object> dataMap, String key) {
		Object valueByKey = MapUtilV2.getValueByKey(dataMap, key);

		if (valueByKey == null) {
			return false;
		}

		return true;
	}
}
