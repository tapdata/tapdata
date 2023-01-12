package io.tapdata.bigquery.util.tool;

import java.util.Collection;
import java.util.Map;

public class Checker {
    public static boolean isEmpty(Object obj){
        if (null == obj) return Boolean.TRUE;
        if (obj instanceof String){
            return "".equals(((String) obj).trim());
        }
        return Boolean.FALSE;
    }
    public static boolean isNotEmpty(Object object){
        return !isEmpty(object);
    }
    public static boolean isEmptyCollection(Object collection){
        return isEmpty(collection)
                        || ( collection instanceof Collection && ((Collection) collection).isEmpty() )
                        || (collection instanceof Map && ((Map) collection).isEmpty())
                ;
    }
    public static boolean isNotEmptyCollection(Object collection){
        return !isEmptyCollection(collection);
    }
}
