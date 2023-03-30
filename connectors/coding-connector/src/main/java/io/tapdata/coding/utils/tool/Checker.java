package io.tapdata.coding.utils.tool;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class Checker {
    public static boolean isEmpty(Object obj) {
        if (null == obj) return Boolean.TRUE;
        if (obj instanceof String) {
            return "".equals(((String) obj).trim());
        }
        return Boolean.FALSE;
    }

    public static boolean isNotEmpty(Object object) {
        return !isEmpty(object);
    }

    public static boolean isEmptyCollection(Object collection) {
        return isEmpty(collection)
                || (collection instanceof Collection && ((Collection<?>) collection).isEmpty())
                || (collection instanceof Map && ((Map<?, ?>) collection).isEmpty())
                ;
    }

    public static boolean isNotEmptyCollection(Object collection) {
        return !isEmptyCollection(collection);
    }

    public static void main(String[] args) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
        String dateStr = formatter.format(new Date(1576477905000L));
        //1576474321000L
        //1576477905000L
        System.out.println(dateStr);
    }
}
