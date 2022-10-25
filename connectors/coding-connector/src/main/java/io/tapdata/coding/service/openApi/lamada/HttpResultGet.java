package io.tapdata.coding.service.openApi.lamada;

import io.tapdata.coding.utils.tool.Checker;

import java.util.Collections;
import java.util.Map;

/**
 * lamada
 * */
public interface HttpResultGet<T> {
    public static final String SPLIT_KEY = "\\.";
    public T result(Map<String,Object> response);

    public static Object getByKey(String key,Map<String,Object> response){
        if (Checker.isEmpty(key)) return response;
        String [] keys = key.split(SPLIT_KEY);
        Map<String,Object> result = response;
        for (String keySplit : keys) {
            if (Checker.isEmptyCollection(result)) return Collections.emptyMap();
            Object splitMap = result.get(keySplit);
            if (null == splitMap) return Collections.emptyMap();
            if (splitMap instanceof Map) {
                result = (Map<String, Object>) splitMap;
            }else {
                return splitMap;
            }

        }
        return result;
    }
}
