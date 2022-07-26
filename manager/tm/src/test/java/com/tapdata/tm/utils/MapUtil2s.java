//package com.tapdata.tm.utils;
//
//import lombok.extern.slf4j.Slf4j;
//
//import java.util.*;
//
//@Slf4j
//public class MapUtil2s {
//
//    /**
//     * 替换指定key对应的value（替换单个）
//     *
//     * @param map
//     * @return
//     */
//    public static Object replace(Map map) {
//
//        Set<String> keys = map.keySet();
////        if (keys.contains(key)) {
//        if (keys.contains("$gt")) {
//            Map gtDateMap = (Map) map.get("$gt");
//            Date gtDate = null;
//            if (gtDateMap.containsKey("$date")) {
//                Double doubleValue = (Double) gtDateMap.get("$date");
//                gtDate = new Date(doubleValue.longValue());
//                gtDateMap.remove("$date");
//                gtDateMap.replace("$gt", gtDate);
//            }
//        }
//        if (keys.contains("$lt")) {
//            Map ltDateMap = (Map) map.get("$lt");
//            Date ltDate = null;
//            if (ltDateMap.containsKey("$date")) {
//                Double doubleValue = (Double) ltDateMap.get("$date");
//                ltDate = new Date(doubleValue.longValue());
//                ltDateMap.remove("$date");
//                ltDateMap.put("$lt", ltDate);
//            }
//            return map;
//        } else {
//            for (String k : keys) {
//                if (map.get(k) instanceof Map) {
//                    Map m = (Map) map.get(k);
//                    replace(m);
//                }
//            }
//        }
//        return map;
//    }
//
//    /**
//     * 替换指定key对应的value（替换所有）
//     *
//     * @param map
//     * @param key
//     * @param value
//     * @return
//     */
//    public static Map replaceAll(Map<String, Object> map, String key, Object value) {
//
//        Set<String> keys = map.keySet();
//        if (keys.contains(key)) {
//            map.put(key, value);
//        } else {
//            for (String k : keys) {
//                if (map.get(k) instanceof Map) {
//                    Map m = (Map) map.get(k);
//                    replaceAll(m, key, value);
//                } else if (map.get(k) instanceof List) {
//                    List m = (List) map.get(k);
//                    for (Object l : m) {
//                        if (l instanceof Map) {
//                            replaceAll((Map) l, key, value);
//                        }
//                    }
//                }
//            }
//        }
//        return map;
//    }
//
//    //获取value
//    public static Object getValue(Map<String, Object> map, String key) {
//
//        Map result = new HashMap();
//        Set<String> keys = map.keySet();
//        if (keys.contains(key)) {
//            result = map;
//        } else {
//            Map<String, Object> m = (Map<String, Object>) map.get(key);
//            for (String subKey : keys) {
//                Object subValue = map.get(subKey);
//                if (null != subValue && subValue instanceof Map) {
//                    getValue((Map<String, Object>) subValue, key);
//                } else {
//                    System.out.println(subKey);
//                }
//            }
//        }
//        return result.get(key);
//    }
//
//    public static Object replace2(Map<String, Object> map) {
//
//        Set<String> keys = map.keySet();
//        if (keys.contains("$gt") || keys.contains("$lt")) {
//            Map gtMap = (Map) map.get("$gt");
//            Map ltMap = (Map) map.get("$lt");
//            if (null != gtMap && gtMap.containsKey("$date")) {
//                Long gtInt =  cn.hutool.core.map.MapUtil.getLong(gtMap,"$date");
//                Date gtDate=new Date(gtInt);
//
//                gtMap.remove("$date");
//                map.replace("$gt", gtDate);
//            }
//            if (null != ltMap && ltMap.containsKey("$date")) {
//                Long ltInt = cn.hutool.core.map.MapUtil.getLong(ltMap,"$date");
//                Date gtDate=new Date(ltInt.longValue());
//                gtMap.remove("$date");
//                map.replace("$lt", gtDate);
//            }
//            return map;
//        } else {
//            for (String k : keys) {
//                if (map.get(k) instanceof Map) {
//                    Map m = (Map) map.get(k);
//                    replace2(m );
//                } else if (map.get(k) instanceof List) {
//                    List m = (List) map.get(k);
//                    for (Object l : m) {
//                        if (l instanceof Map) {
//                            replace2((Map) l);
//                        }
//                    }
//                }
//            }
//        }
//
//        return map;
//    }
//
//
//}