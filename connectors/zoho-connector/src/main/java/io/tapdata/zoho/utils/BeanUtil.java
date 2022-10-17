package io.tapdata.zoho.utils;

public class BeanUtil {
    public static <T>T bean(String packageName){
        if (Checker.isEmpty(packageName)) return null;
        Class clz = null;
        try {
            clz = Class.forName(packageName);
            return ((T)clz.newInstance());
        } catch (ClassNotFoundException e) {
            //e.printStackTrace();
        } catch (InstantiationException e1) {
            //e1.printStackTrace();
        } catch (IllegalAccessException e2) {
            //e2.printStackTrace();
        }
        return null;
    }
}
