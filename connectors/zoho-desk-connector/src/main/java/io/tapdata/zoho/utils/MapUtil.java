package io.tapdata.zoho.utils;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONNull;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.zoho.enums.Constants;

import java.util.*;

import static io.tapdata.base.ConnectorBase.entry;
import static io.tapdata.base.ConnectorBase.map;

public class MapUtil {
    public final static String DOT = ".";
    public final static String DOT_REGEX = "\\.";

    /**
     * 根据Key去除value，如果是指定Map，就完整拍平，以key为做所有子属性的前缀，不是Map类型先不操作
     * */
    public static void fullFlat(Map<String,Object> map,String key){
        if (null == map || map.isEmpty()) return;
        Object value = map.get(key);
        if (null == value) return;
        if (value instanceof Map){
            map.remove(key);
            Map<String,Object> keyMap = (Map<String,Object>)value;
            keyMap.forEach((k,v)->map.put(key+"_"+k,v));
        }
    }
    /**
     * 把指定key的值转String
     * */
    public static void valueToString(Map<String,Object> map,String key){
        if (null == map || map.isEmpty()) return;
        Object value = map.get(key);
        if (null == value) return;
        map.put(key, JSONUtil.toJsonStr(value));
    }

    public static void main(String[] args) {
        Map<String,Object> map = new HashMap<String,Object>(){{
            put("key","1");
            put("key2",new ArrayList<Map<String,Object>>(){{
                put("key",112);
                put("key2","22");
            }});
            put("key3",new HashMap<String,Object>(){{
                put("key1",111);
                put("key2",222);
            }});
        }};
        long start = System.currentTimeMillis();
        for (long i = 0; i < 1000000000L; i++) {
            map.put("key3",JSONUtil.toJsonStr(map.get("key3")));
        }
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(map.get("key3"));
    }
    /**
     * 根据点号分割向下获取值，结果的key遇点第一个字母大写并去除逗号
     * **/
    public static void putMapSplitByDotKeyNameFirstUpper(Map<String,Object> map, Map<String,Object> targetMap, String ... keys){
        putMapKeyNameFirstUpper(MapUtil.DOT,map,targetMap,keys);
    }
    /**
     * 根据指定符号号分割向下获取值，结果的key遇符号第一个字母大写并去除逗号
     * **/
    public static void putMapKeyNameFirstUpper(String spilt,Map<String,Object> map, Map<String,Object> targetMap, String ... keys){
        for (String key : keys) {
            Map<String, Object> entry = map(entry(ZoHoString.fistUpper(key,spilt),getValue(key, spilt, map)));
            if (Checker.isNotEmpty(entry)) {
                targetMap.putAll(entry);
            }
        }
    }
    /**
     * 根据指定符号号分割向下获取值，key保留作为结果的key
     * {
     *     "${finalKey}":${value}
     * }
     * */
    public static void putMap(String spilt,Map<String,Object> map, Map<String,Object> targetMap, String ... keys){
        for (String key : keys) {
            Map<String, Object> entry = map(entry(key,getValue(key, spilt, map)));
            if (Checker.isNotEmpty(entry)) {
                targetMap.putAll(entry);
            }
        }
    }
    /**
     * {
     *     "${key}":{
     *         "${dataKey}":${value}
     *     }
     * }
     * */
    public static void asMapPutOntoMap(String spilt,Map<String,Object> map, Map<String,Object> targetMap, String key,String dataKey){
        Map<String, Object> entry = getEntryAsDataMap(key,dataKey,key,spilt,map);
        if (Checker.isNotEmpty(entry)) {
            targetMap.putAll(entry);
        }
    }
    private static Map<String,Object> getEntryAsDataMap(
            final String finalKey,
            final String valueKey,
            String key,
            String split,
            Map<String,Object> map){
        return map(entry(finalKey,map(entry(valueKey,getValue(key, split, map)))));
    }
    private static Object getValue(String key,String split,Map<String,Object> map){
        int index = key.contains(split)?key.indexOf(split):key.length();
        String currentKey = key.substring(0, index);
        Object value = map.get(currentKey);
        if (Checker.isEmpty(value)){
            return null;//@TODO null
        }
        if (index<key.length()-1){
            String nextKey = key.substring(index + 1);
            if (value instanceof JSONNull){
                return null;
            }
            else if ((value instanceof JSONObject) || (value instanceof Map)){
                Map<String,Object> obj = (Map<String,Object>)value;
                return getValue(nextKey,split,obj);
            }else if(value instanceof JSONArray || value instanceof Collection){
                try {
                    Collection<Map<String, Object>> list = (Collection<Map<String, Object>>) value;
                    StringJoiner joiner = new StringJoiner(Constants.ARRAY_SPLIT_CHAR);
                    list.forEach(l->{
                        Object nextKeyObj = l.get(nextKey);
                        if (Checker.isNotEmpty(nextKeyObj)) {
                            joiner.add(String.valueOf(nextKeyObj));
                        }
                    });
                    return joiner.toString();
                }catch (Exception e){
                    //@TODO 多层list嵌套时目前不支持解析，返回null
                    return "";//@TODO null
                    //List<List<Object>> catchList = (List<List<Object>>) value;
                    //catchList.forEach(list->{
                    //
                    //});
                }
            }else {
                return null;//@TODO null
            }
        }
        return value;//@TODO null
    }
}
