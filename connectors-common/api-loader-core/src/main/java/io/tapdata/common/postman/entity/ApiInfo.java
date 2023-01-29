package io.tapdata.common.postman.entity;


import java.util.HashMap;
import java.util.Map;

public class ApiInfo<K,V> extends HashMap {
    public static ApiInfo create(){
        return new ApiInfo<>();
    }
    public static ApiInfo create(Map info){
        ApiInfo apiInfo = new ApiInfo<>();
       apiInfo.putAll(info);
       return apiInfo;
    }
    public ApiInfo(Map<K,V> info){
        super();
        putAll(info);
    }
    public ApiInfo(){

    }
}
