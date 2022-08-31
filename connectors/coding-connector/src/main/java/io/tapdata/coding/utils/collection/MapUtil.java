package io.tapdata.coding.utils.collection;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import java.util.Map;

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
}
