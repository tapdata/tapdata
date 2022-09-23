package io.tapdata.zoho.utils;

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
