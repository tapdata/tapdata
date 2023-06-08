package io.tapdata.http.util;

/**
 * @author GavinXiao
 * @description Check create by Gavin
 * @create 2023/5/17 18:36
 **/
public class Checker {
    private Checker(){}
    public static Checker create(){
        return new Checker();
    }

    public static boolean isEmpty(Object obj){
        return null == obj ? Boolean.TRUE : ( obj instanceof String ? "".equals(((String)obj).trim()) : Boolean.FALSE );
    }

    public static boolean isNotEmpty(Object obj){
        return !isEmpty(obj);
    }
}
