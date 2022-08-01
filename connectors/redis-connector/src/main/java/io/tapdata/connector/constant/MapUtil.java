package io.tapdata.connector.constant;

import com.alibaba.fastjson.JSONObject;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;


import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;


public class MapUtil {


  public static void copyToNewMap(Map map, Map newMap) {

    if (map == null) {
      return;
    }

    for (Object obj : map.entrySet()) {
      Map.Entry entry = (Map.Entry) obj;
      Object key = entry.getKey();
      Object value = entry.getValue();

      if (value instanceof ScriptObjectMirror &&
        ((ScriptObjectMirror) value).isArray()
      ) {
        List list = ((ScriptObjectMirror) value).to(List.class);
        List newObject = new ArrayList();
        ListUtil.copyList(list, newObject);
        newMap.put(key, newObject);
      } else if (value instanceof Map) {
        Map newObject = new HashMap();
        copyToNewMap((Map) value, newObject);
        newMap.put(key, newObject);

      } else if (value instanceof List) {
        List newObject = new ArrayList();
        ListUtil.copyList((List) value, newObject);
        newMap.put(key, newObject);
      }  else {
        newMap.put(key, JSONObject.toJSONString(value));
      }
    }

  }


  public static void deepCloneMap(Map map, Map newMap) throws IllegalAccessException, InstantiationException {

    if (map == null) {
      return;
    }

    for (Object obj : map.entrySet()) {
      Map.Entry entry = (Map.Entry) obj;
      Object key = entry.getKey();
      Object value = entry.getValue();
      // recursive map
      if (value instanceof Map) {
        Map newObject = (Map) value.getClass().newInstance();
        deepCloneMap((Map) value, newObject);
        newMap.put(key, newObject);
      } else if (value instanceof List) {
        List newObject = (List) value.getClass().newInstance();
        ListUtil.serialCloneList((List) value, newObject);
        newMap.put(key, newObject);
      } else if (value instanceof Serializable) {
        Serializable serl = (Serializable) value;
        Serializable clone = SerializationUtils.clone(serl);
        newMap.put(key, clone);
      } else {
        newMap.put(key, value);
      }
    }

  }

}
