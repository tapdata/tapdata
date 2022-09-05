package io.tapdata.coding.utils.collection;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import java.util.ArrayList;
import java.util.HashMap;
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

    public static void main(String[] args) {
        Map<String,Object> map = new HashMap<>();
        map.put("key1",new ArrayList<Object>(){{
            add("va");
            add("val");
            add(1);
        }});
        map.put("key2",100000l);
        map.put("key3",new HashMap<String,Object>(){{
            put("key1-1",1);
            put("key1-2","hello");
        }});

        System.out.println(map.toString());

        JSONObject jsonObject = JSONUtil.parseObj(map);
        System.out.println(jsonObject.toString());
    }
}
