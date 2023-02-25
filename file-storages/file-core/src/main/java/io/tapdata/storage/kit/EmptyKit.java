package io.tapdata.storage.kit;

import java.util.Collection;
import java.util.Map;

/**
 * Empty tools for String, Collection, Map, String, Array (Blank > Empty > Null)
 *
 * @author Jarad
 * @date 2022/4/29
 */
public class EmptyKit {

    public static boolean isEmpty(Collection<?> var) {
        return null == var || var.isEmpty();
    }

    public static boolean isEmpty(Map<?, ?> var) {
        return null == var || var.isEmpty();
    }

    public static boolean isEmpty(String var) {
        return null == var || var.isEmpty();
    }

    public static <T> boolean isEmpty(T[] arr) {
        return null == arr || arr.length == 0;
    }

    public static boolean isNotEmpty(Collection<?> var) {
        return null != var && !var.isEmpty();
    }

    public static boolean isNotEmpty(Map<?, ?> var) {
        return null != var && !var.isEmpty();
    }

    public static boolean isNotEmpty(String var) {
        return null != var && !var.isEmpty();
    }

    public static <T> boolean isNotEmpty(T[] arr) {
        return null != arr && arr.length > 0;
    }

    public static boolean isBlank(String var) {
        return null == var || var.isEmpty() || var.trim().isEmpty();
    }

    public static boolean isNotBlank(String var) {
        return null != var && !var.isEmpty() && !var.trim().isEmpty();
    }

    public static boolean isNull(Object var) {
        return null == var;
    }

    public static boolean isNotNull(Object var) {
        return null != var;
    }

}
