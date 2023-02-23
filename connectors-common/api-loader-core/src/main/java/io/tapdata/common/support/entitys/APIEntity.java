package io.tapdata.common.support.entitys;

import java.io.Serializable;

public class APIEntity implements Serializable {
    private static final long serialversionUID = 1L;
    String name;
    String url;
    String method;
    public static APIEntity create(String name, String url,String method){
        APIEntity entity = new APIEntity();
        return entity.method(method).name(name).url(url);
    }
    public String url(){
        return this.url;
    }
    public String name(){
        return this.name;
    }
    public String method(){
        return this.method;
    }
    public APIEntity name(String name){
        this.name = name;
        return this;
    }
    public APIEntity url(String url){
        this.url = url;
        return this;
    }
    public APIEntity method(String method){
        this.method = method;
        return this;
    }
}
