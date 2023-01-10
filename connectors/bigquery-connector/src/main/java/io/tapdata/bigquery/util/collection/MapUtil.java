package io.tapdata.bigquery.util.collection;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.bigquery.enums.Constants;

import java.util.Map;
import java.util.StringJoiner;

public class MapUtil<K,V> {
    public static MapUtil create(){
        return new MapUtil<>();
    }
    public Integer hashCode(Map<K,V> map){
        if (null == map) return -1;
        return toString(map).hashCode();
    }

    private String toString(Map<K,V> map){
        if (null == map) return "";
        if (map.size()==0) return map.toString();
        JSONObject jsonObject = JSONUtil.parseObj(map);
        return jsonObject.toString();
    }
    public static String CSVTitle(Map<String,Object> filedMap){
        StringJoiner joiner = new StringJoiner(Constants.SPLIT_CHAR);
        title(joiner,filedMap,null);
        return joiner.toString();
    }
    public static void title(StringJoiner joiner,Map<String,Object> filedMap,String key){
        for (final Map.Entry<String,Object> entry : filedMap.entrySet()){
            String entryKey = entry.getKey();
            Object entryObj = entry.getValue();
            if (entryObj instanceof JSONObject){
                title(joiner,(Map<String,Object>)entryObj,(null!=key?key+"_":"")+entryKey);
            }else {
                if (null != entryKey) {
                    String line = (null!=key?key+"_":"") + String.valueOf(entryKey);
                    joiner.add(line);
                }
            }
        }
    }

    public static String CSVLine(Map<String,Object> csvObj,Map<String,Object> filedMap){
        StringJoiner joiner = new StringJoiner(Constants.SPLIT_CHAR);
        join(joiner,csvObj,filedMap);
        return joiner.toString();
    }
    public static void join(StringJoiner joiner,Map<String,Object> csvObj,Map<String,Object> filedMap){
        for (final Map.Entry<String,Object> entry : filedMap.entrySet()){
            String entryKey = entry.getKey();
            Object value = csvObj.getOrDefault(entryKey, Constants.NULL_VALUE);
            if (value instanceof JSONObject){
                join(joiner,(Map<String,Object>)value,(Map<String,Object>)filedMap.get(entryKey));
            }else {
                if (null == value) {
                    value = Constants.NULL_VALUE;
                }
                String line = String.valueOf(value);
                if ("Description".equals(entryKey) || line.contains(Constants.LINE_DELIMITER_DEFAULT) || line.contains(Constants.LINE_DELIMITER_DEFAULT_2)) {
                    line = Constants.ESCAPE_CHARACTER_DOUBLE_QUOTATION + value + Constants.ESCAPE_CHARACTER_DOUBLE_QUOTATION;
                }
                joiner.add(line);
            }
        }
    }
}
