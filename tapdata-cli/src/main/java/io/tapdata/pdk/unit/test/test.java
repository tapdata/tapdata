package io.tapdata.pdk.unit.test;

import io.tapdata.entity.schema.TapTable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class test {
    static Thread thread;

    public static void main(String... args) throws Throwable {
        int count = 10_000_000;
//		int count = 1;

        Map<String, Runnable> stringMap = new ConcurrentHashMap<>();
        stringMap.put("aaaaaa", new Runnable() {
            @Override
            public void run() {

            }
        });
        Map<Class<?>, Runnable> classMap = new ConcurrentHashMap<>();
        classMap.put(TapTable.class, new Runnable() {
            @Override
            public void run() {

            }
        });

        TapTable obj = new TapTable("a");
        long time;
        time = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            } else if (obj instanceof TapTable) {
            }
        }
        System.out.println("instanceof takes " + (System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            if (stringMap.containsKey(obj.getClass().getName())) {
            }
        }
        System.out.println("getName takes " + (System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            if (classMap.containsKey(obj.getClass())) {
            }
            {
            }
        }
        System.out.println("getClass takes " + (System.currentTimeMillis() - time));

    }
}
