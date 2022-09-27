package io.tapdata.zoho.utils;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONNull;
import cn.hutool.json.JSONObject;
import io.tapdata.zoho.enums.Constants;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static io.tapdata.base.ConnectorBase.entry;
import static io.tapdata.base.ConnectorBase.map;

public class MapUtil {
    public final static String DOT = ".";
    public final static String DOT_REGEX = "\\.";
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
            return "";//@TODO null
        }
        if (index<key.length()-1){
            String nextKey = key.substring(index + 1);
            if (value instanceof JSONNull){
                return "";
            }
            else if ((value instanceof JSONObject) || (value instanceof Map)){
                Map<String,Object> obj = (Map<String,Object>)value;
                return getValue(nextKey,split,obj);
            }else if(value instanceof JSONArray || value instanceof List){
                try {
                    List<Map<String, Object>> list = (List<Map<String, Object>>) value;
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
                return "";//@TODO null
            }
        }
        return null == value ? "":value;//@TODO null
    }
}
