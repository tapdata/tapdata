package io.tapdata.common.postman.entity;

import io.tapdata.common.postman.enums.PostParam;

import java.util.*;

public class ApiVariable<K,V> extends HashMap {

    public static ApiVariable create(){
        return new ApiVariable();
    }

    public static ApiVariable create(List variable){
        ApiVariable variableMap = new ApiVariable();
        variable.stream().filter(Objects::nonNull).forEach(var->{
            String key = (String)((Map<String,Object>)var).get(PostParam.KEY);
            variableMap.put(key,var);
        });
        return variableMap;
    }
}
