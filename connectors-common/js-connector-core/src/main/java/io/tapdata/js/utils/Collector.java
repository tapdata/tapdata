package io.tapdata.js.utils;

import java.util.*;

public class Collector {
    public static final String[] ignore = new String[]{"java.util"};

    /**
     * @deprecated for test
     * */
    private static boolean needIgnore(Object obj) {
        if (null == obj) return true;
        for (String ig : ignore) {
            if (obj.getClass().getPackage().getName().startsWith(ig)) return false;
        }
        return true;
    }

    public static Map<?, ?> convertMap(Map<?, ?> map) {
        if (map == null) return null;
        else if (map.isEmpty()) return new HashMap<>();
        else {
            Map<Object, Object> ent = map instanceof LinkedHashMap ? new LinkedHashMap<>() : new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey();
                Object value = entry.getValue();
                ent.put(Collector.convertObj(key), Collector.convertObj(value));
            }
            return ent;
        }
    }

    public static List<?> convertList(List<?> list) {
        if (list == null) return null;
        else if (list.isEmpty()) return new ArrayList<>();
        else {
            List<Object> ent = new ArrayList<>();
            for (Object li : list) {
                ent.add(Collector.convertObj(li));
            }
            return ent;
        }
    }

    public static Object[] convertArr(Object[] arr) {
        if (arr == null) return null;
        else if (arr.length == 0) return new Object[0];
        else {
            Object[] ent = new Object[arr.length];
            for (int i = 0; i < arr.length; i++) {
                ent[i] = Collector.convertObj(arr[i]);
            }
            return ent;
        }
    }

    public static Object convertObj(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof Map) {
            return obj instanceof LinkedHashMap ? new LinkedHashMap<>(Collector.convertMap((Map<?, ?>) obj)) : new HashMap<>(Collector.convertMap((Map<?, ?>) obj));
        } else if (obj instanceof Collection) {
            return new ArrayList<>(Collector.convertList((List<?>) obj));
        } else if (obj.getClass().isArray()) {
            return Collector.convertArr((Object[]) obj);
        } else {
            return obj;
        }
    }

    /**
     * @deprecated for test
     * */
    public static List<?> listSimple(int num) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            list.add((new Random()).nextFloat());
        }
        return list;
    }

    /**
     * @deprecated for test
     * */
    public static Map<?, ?> mapSimple(int num) {
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < num; i++) {
            map.put((new Random()).nextFloat(), System.currentTimeMillis());
        }
        return map;
    }

    /**
     * @deprecated for test
     * */
    public static Object[] arrSimple(int num) {
        Object[] arr = new Object[num];
        for (int i1 = 0; i1 < num; i1++) {
            arr[i1] = i1;
        }
        return arr;
    }

    public static void main(String[] args) {
        List<Object> testList = new ArrayList<>();
        for (int i = 1; i < 1000000; i++) {
            if (i % 5 == 0) {
                testList.add(mapSimple(5));
            } else if (i % 3 == 0) {
                testList.add(listSimple(12));
            } else if (i % 4 == 0) {
                testList.add(arrSimple(24));
            } else if (i % 7 == 0) {
                testList.add(i);
            } else {
                testList.add(UUID.randomUUID().toString());
            }
        }
        System.out.println("start convert...");
        long start = System.currentTimeMillis();
        List<?> convertList = (List<?>) Collector.convertObj(testList);
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
