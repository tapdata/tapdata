package io.tapdata.connector.hazelcast.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.map;

/**
 * Author:Skeet
 * Date: 2023/2/8
 **/
public class ObjectKey implements Serializable {
    public static Map<String, Object> getKeyFromData(Map<String, Object> before, Map<String, Object> after, Collection<String> primaryFields) {
        Map<String, Object> map = map();
        if (before != null) {
            for (String field : primaryFields) {
                map.put(field, before.get(field));
            }
        } else if (after != null) {
            for (String field : primaryFields) {
                map.put(field, after.get(field));
            }
        }
        return map;
    }
}
