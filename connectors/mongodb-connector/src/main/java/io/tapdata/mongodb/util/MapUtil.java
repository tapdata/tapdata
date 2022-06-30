package io.tapdata.mongodb.util;

import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.MongodbUtil;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.Document;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MapUtil {
  /**
   * get value from map, before target
   *
   * @param dataMap
   * @param key
   * @return
   * @throws NullPointerException
   */
  public static Object getValueByKey(Map<String, Object> dataMap, String key) throws NullPointerException {
    return getValueByKey(dataMap, key, "");
  }

  /**
   * get value from map in target
   *
   * @param dataMap
   * @param key
   * @param replacement
   * @return
   */
  public static Object getValueByKey(Map<String, Object> dataMap, String key, String replacement) {
    Object value = null;

    if (MapUtils.isEmpty(dataMap) || EmptyKit.isBlank(key)) {
      return null;
    }

    if (needSplit(key)) {
      String[] split = key.split("\\.");

      if (split.length > 0 && EmptyKit.isNotEmpty(split)) {

        List<String> keys = Arrays.stream(split).filter(EmptyKit::isNotBlank).collect(Collectors.toList());

        value = dataMap;
        for (int i = 0; i < keys.size(); i++) {
          String subKey = keys.get(i);

          subKey = EmptyKit.isNotBlank(replacement) ? MongodbUtil.mongodbKeySpecialCharHandler(subKey, replacement) : subKey;

          if (value instanceof Map && ((Map) value).containsKey(subKey)) {
            value = ((Map) value).get(subKey);
          } else {
            value = null;
            break;
          }
        }
      }
    }

    key = trimKey(key);

    if (value == null) {
      key = EmptyKit.isNotBlank(replacement) ? MongodbUtil.mongodbKeySpecialCharHandler(key, replacement) : key;
      if (dataMap.containsKey(key)) {
        value = dataMap.get(key);
      }
    }

    return value;
  }

  /**
   * 通过特殊符号"."判断是否是多层级的字段
   *
   * @param key
   * @return
   */
  public static boolean needSplit(String key) {
    return key.contains(".") && !key.startsWith(".") && !key.endsWith(".");
  }

  /**
   * 去除key的双引号,oracle列名可能携带双引号
   *
   * @param key
   * @return
   */
  private static String trimKey(String key) {
    if (key.contains("\"") && key.startsWith("\"") && key.endsWith("\"")) {
      key = key.replace("\"", "");
    }
    return key;
  }

  public static boolean removeValueByKey(Map<String, Object> dataMap, String key, String replacement) {
    boolean isRemoved = false;
    if (MapUtils.isNotEmpty(dataMap) && EmptyKit.isNotBlank(key)) {
      if (needSplit(key)) {
        String[] split = key.split("\\.");

        if (split.length > 0 && EmptyKit.isNotEmpty(split)) {
          Object value = dataMap;
          for (int i = 0; i < split.length; i++) {
            String subKey = split[i];
            if (EmptyKit.isNotBlank(replacement)) {
              subKey = MongodbUtil.mongodbKeySpecialCharHandler(subKey, replacement);
            }

            if (value instanceof Map && ((Map) value).containsKey(subKey)) {
              if (i < split.length - 1) {
                value = ((Map) value).get(subKey);
              } else {
                ((Map) value).remove(subKey);
                isRemoved = true;
              }
            }
          }
        }
      }
      if (!isRemoved) {
        if (EmptyKit.isNotBlank(replacement)) {
          key = MongodbUtil.mongodbKeySpecialCharHandler(key, replacement);
        }
        if (dataMap.containsKey(key)) {
          dataMap.remove(key);
          isRemoved = true;
        }
      }
    }

    return isRemoved;
  }

  public static boolean containsKey(Map<String, Object> dataMap, String key) {
    if (MapUtils.isEmpty(dataMap)) {
      return false;
    }

    if (dataMap.containsKey(key)) {
      return true;
    }

    if (EmptyKit.isBlank(key)) {
      return false;
    }

    String[] split = key.split("\\.");
    for (int i = 0; i < split.length; i++) {
      String nestKey = split[i];

      if (!dataMap.containsKey(nestKey)) {
        return false;
      }

      if (i == split.length - 1) {
        return true;
      }

      Object value = dataMap.get(nestKey);
      if (value instanceof Map) {
        dataMap = (Map<String, Object>) value;
      }
    }

    return false;
  }

  public static boolean removeValueByKey(Map<String, Object> dataMap, String key) {
    return removeValueByKey(dataMap, key, "");
  }

  public static Map<String, Object> obj2Map(Object obj) throws IllegalAccessException {
    Map<String, Object> map = new LinkedHashMap<>();
    if (obj == null) {
      return map;
    }
    Class<?> clazz = obj.getClass();
    for (Field field : clazz.getDeclaredFields()) {
      field.setAccessible(true);
      String fieldName = field.getName();
      Object value = field.get(obj);
      if (value == null) {
        continue;
      }
      map.put(fieldName, value);
    }
    return map;
  }

  public static Document obj2Document(Object obj) throws IllegalAccessException {
    Document document = new Document();
    if (obj == null) {
      return document;
    }
    Class<?> clazz = obj.getClass();
    for (Field field : clazz.getDeclaredFields()) {
      field.setAccessible(true);
      String fieldName = field.getName();
      Ignore annotation = field.getAnnotation(Ignore.class);
      if (annotation != null) {
        continue;
      }
      Object value = field.get(obj);
      if (value == null) {
        continue;
      }
      document.put(fieldName, value);
    }
    return document;
  }

  public static void removeKey(Object map, String key) {
    int index = key.indexOf(".");
    String keyNow = key;
    if (index > 0) {
      keyNow = key.substring(0, index);
    }
    if (map instanceof Map && ((Map) map).containsKey(keyNow)) {
      if (index > 0) {
        key = key.substring(index + 1);
        map = ((Map) map).get(keyNow);
        removeKey(map, key);
      } else {
        ((Map) map).remove(key);
      }
    } else if (map instanceof List) {
      for (Object o : (List) map) {
        removeKey(o, key);
      }
    }

  }

  public static void retainKey(Object newMap, Object oldMap, String key) {
    int index = key.indexOf(".");
    String keyNow = key;
    if (index > 0) {
      keyNow = key.substring(0, index);
    }
    if (oldMap instanceof Map && ((Map) oldMap).containsKey(keyNow)) {
      if (index > 0) {
        key = key.substring(index + 1);
        oldMap = ((Map) oldMap).get(keyNow);
        Object object = null;
        if (((Map) newMap).containsKey(keyNow)) {
          object = ((Map) newMap).get(keyNow);
        } else {
          if (oldMap instanceof Map) {
            object = new HashMap<>();
          } else if (oldMap instanceof List) {
            object = new ArrayList<>();
          }
          ((Map) newMap).put(keyNow, object);
        }
        retainKey(object, oldMap, key);
      } else {
        ((Map) newMap).put(key, ((Map) oldMap).get(key));
      }
    } else if (oldMap instanceof List) {
      for (int i = 0; i < ((List) oldMap).size(); i++) {
        Object o = ((List) oldMap).get(i);
        Object object = null;
        if (((List) newMap).size() > i) {
          object = ((List) newMap).get(i);
        } else {
          if (o instanceof Map) {
            object = new HashMap<>();
          } else if (o instanceof List) {
            object = new ArrayList<>();
          }
          ((List) newMap).add(object);
        }
        retainKey(object, o, key);
      }
    }

  }

  public static void replaceKey(String oldKey, Object map, String newkey) {
    int index = oldKey.indexOf(".");
    String oldKeyNow = oldKey;
    if (index > 0) {
      oldKeyNow = oldKey.substring(0, index);
    }
    if (map instanceof Map && ((Map) map).containsKey(oldKeyNow)) {
      if (index > 0) {
        oldKey = oldKey.substring(index + 1);
        map = ((Map) map).get(oldKeyNow);
        replaceKey(oldKey, map, newkey);
      } else {
        ((Map) map).put(newkey, ((Map) map).get(oldKey));
        ((Map) map).remove(oldKey);
      }
    } else if (map instanceof List) {
      for (Object o : (List) map) {
        replaceKey(oldKey, o, newkey);
      }
    }

  }

  public static void recursiveRemoveKey(Map<String, Object> map, String removeKey) {
    if (MapUtils.isNotEmpty(map)) {

      final Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
      while (iterator.hasNext()) {
        final Map.Entry<String, Object> entry = iterator.next();
        final String key = entry.getKey();
        if (key.equals(removeKey)) {
          iterator.remove();
        }

        Object o = entry.getValue();
        if (o instanceof Map) {
          recursiveRemoveKey((Map) o, removeKey);
        } else if (o instanceof List) {
          List list = (List) o;
          if (CollectionUtils.isNotEmpty(list)) {
            for (Object o1 : list) {
              if (o1 instanceof Map) {
                recursiveRemoveKey((Map) o1, removeKey);
              }
            }
          }
        }
      }
    }
  }

  public static void recursiveRemoveByValuePredicate(Map<String, Object> map, Predicate<Object> valuePredicate) {
    if (MapUtils.isNotEmpty(map)) {

      final Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
      while (iterator.hasNext()) {

        final Map.Entry<String, Object> entry = iterator.next();
        Object o = entry.getValue();
        if (valuePredicate.test(o)) {
          iterator.remove();
        } else if (o instanceof Map) {

          recursiveRemoveByValuePredicate((Map) o, valuePredicate);
        } else if (o instanceof List) {
          List list = (List) o;
          if (CollectionUtils.isNotEmpty(list)) {
            for (Object o1 : list) {
              if (o1 instanceof Map) {
                recursiveRemoveByValuePredicate((Map) o1, valuePredicate);
              }
            }
          }
        }
      }
    }
  }

  public static void removeNullValue(Map<String, Object> map) {
    if (MapUtils.isNotEmpty(map)) {

      recursiveRemoveByValuePredicate(map, o -> {
        if (o == null) {
          return true;
        } else {
          if (o instanceof Double) {
            return Double.isNaN((Double) o);
          }
        }

        return false;
      });
    }
  }

  public static Map<String, Object> keyToLowerCase(Map<String, Object> map) {
    Map<String, Object> returnMap = new HashMap<>();

    if (MapUtils.isEmpty(map)) {
      return returnMap;
    }

    Iterator<String> iterator = map.keySet().iterator();
    while (iterator.hasNext()) {
      String key = iterator.next();
      Object value = map.get(key);
      if (value == null) {
        returnMap.put(key.toLowerCase(), null);
        continue;
      }

      if (value instanceof Map) {
        value = keyToLowerCase((Map<String, Object>) value);
      } else if (value instanceof List) {
        ListIterator<Object> listIterator = ((List<Object>) value).listIterator();
        while (listIterator.hasNext()) {
          Object listValue = listIterator.next();
          if (listValue == null) {
            continue;
          }
          if (listValue instanceof Map) {
            Map<String, Object> listMap = keyToLowerCase((Map<String, Object>) listValue);
            listIterator.set(listMap);
          }
        }
      }

      returnMap.put(key.toLowerCase(), value);
    }

    return returnMap;
  }

  public static Map<String, Object> keyToUpperCase(Map<String, Object> map) {
    Map<String, Object> returnMap = new HashMap<>();

    if (MapUtils.isEmpty(map)) {
      return returnMap;
    }

    Iterator<String> iterator = map.keySet().iterator();
    while (iterator.hasNext()) {
      String key = iterator.next();
      Object value = map.get(key);
      if (value == null) {
        returnMap.put(key.toUpperCase(), null);
        continue;
      }

      if (value instanceof Map) {
        value = keyToUpperCase((Map<String, Object>) value);
      } else if (value instanceof List) {
        ListIterator<Object> listIterator = ((List<Object>) value).listIterator();
        while (listIterator.hasNext()) {
          Object listValue = listIterator.next();
          if (listValue == null) {
            continue;
          }
          if (listValue instanceof Map) {
            Map<String, Object> listMap = keyToUpperCase((Map<String, Object>) listValue);
            listIterator.set(listMap);
          }
        }
      }

      returnMap.put(key.toUpperCase(), value);
    }

    return returnMap;
  }

  public static Map<String, Object> keyToLowerCase(Map<String, Object> map, int layerCounts, int layerIndex) {
    Map<String, Object> returnMap = new HashMap<>();

    if (MapUtils.isEmpty(map)) {
      return returnMap;
    }

    Iterator<String> iterator = map.keySet().iterator();
    while (iterator.hasNext()) {
      String key = iterator.next();
      Object value = map.get(key);
      if (value == null) {
        returnMap.put(key.toLowerCase(), null);
        continue;
      }

      if (layerCounts < 1 || layerIndex < layerCounts) {
        if (value instanceof Map) {
          value = keyToLowerCase((Map<String, Object>) value, layerCounts, layerIndex + 1);
        } else if (value instanceof List) {
          ListIterator<Object> listIterator = ((List<Object>) value).listIterator();
          while (listIterator.hasNext()) {
            Object listValue = listIterator.next();
            if (listValue == null) continue;
            if (listValue instanceof Map) {
              Map<String, Object> listMap = keyToLowerCase((Map<String, Object>) listValue, layerCounts, layerIndex + 2);
              listIterator.set(listMap);
            }
          }
        }
      }

      returnMap.put(key.toLowerCase(), value);
    }

    return returnMap;
  }

  public static Set<String> getAllKeys(Map map, boolean recursive) {
    Set<String> keys = new LinkedHashSet<>();

    if (MapUtils.isEmpty(map)) {
      return keys;
    }

    if (!recursive) {
      keys = map.keySet();
      return keys;
    }

    Iterator<String> keyIter = map.keySet().iterator();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      Object value = map.get(key);

      keys.add(key);

      if (value == null) {
        continue;
      }

      if (value instanceof Map) {
        recursiveMapGetKeys((Map<String, Object>) value, keys, key);
      } else if (value instanceof List) {
        recursiveListGetKeys((List) value, keys, key);
      }
    }

    return keys;
  }

  private static void recursiveMapGetKeys(Map<String, Object> subMap, Set<String> keys, String parentKey) {
    Iterator<String> keyIter = subMap.keySet().iterator();

    while (keyIter.hasNext()) {
      String key = keyIter.next();
      Object value = subMap.get(key);

      keys.add(parentKey + "." + key);

      Optional.ofNullable(value).ifPresent(v -> {
        if (v instanceof Map) {
          recursiveMapGetKeys((Map<String, Object>) v, keys, parentKey + "." + key);
        } else if (v instanceof List) {
          recursiveListGetKeys((List) v, keys, parentKey + "." + key);
        }
      });
    }
  }

  private static void recursiveListGetKeys(List list, Set<String> keys, String parentKey) {
    for (int i = 0; i < list.size(); i++) {
      Object value = list.get(i);

      Optional.ofNullable(value).ifPresent(v -> {
        if (v instanceof Map) {
          recursiveMapGetKeys((Map<String, Object>) v, keys, parentKey);
        } else if (v instanceof List) {
          recursiveListGetKeys((List) v, keys, parentKey);
        }
      });
    }
  }

		public static void recursiveFlatMap(Map<String, Object> dataMap, Map<String, Object> newMap, String parentKey) {
				if (MapUtils.isNotEmpty(dataMap)) {
						dataMap.forEach((key, value) -> {
								String fieldName = getFieldName(parentKey, key);
								if (value instanceof Map) {
										recursiveFlatMap((Map) value, newMap, fieldName);
								} else {
										newMap.put(fieldName, value);
								}
						});
				}
		}

		private static String getFieldName(String parentKey, String key) {
				String fieldName;
				if (EmptyKit.isNotBlank(parentKey)) {
						fieldName = parentKey + "." + key;
				} else {
						fieldName = key;
				}
				return fieldName;
		}
}
