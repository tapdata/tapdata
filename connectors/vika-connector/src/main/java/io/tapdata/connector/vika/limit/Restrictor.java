package io.tapdata.connector.vika.limit;

/**
 * @author GavinXiao
 * @description Restrictor create by Gavin
 * @create 2023/6/27 16:02
 **/
public interface Restrictor {
    public static Object limitRule(Restrictor limit) {
        if (null == limit) return null;
        long limitStart = System.currentTimeMillis();
        Object obj = limit.limit();
        long payTime = (System.currentTimeMillis() - limitStart) / 1000;
        try {
            Thread.sleep(200 - payTime);
        }catch (Exception e){
            e.printStackTrace();
        }
        return obj;
    }
    public static void limitRule0(Restrictor0 limit) {
        if (null == limit) return;
        long limitStart = System.currentTimeMillis();
        limit.limit();
        long payTime = (System.currentTimeMillis() - limitStart) / 1000;
        try {
            Thread.sleep(200 - payTime);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public Object limit();
}


