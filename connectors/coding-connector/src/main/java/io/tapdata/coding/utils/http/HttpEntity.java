package io.tapdata.coding.utils.http;

import java.util.HashMap;
import java.util.Map;

public class HttpEntity<String,V> {
    private Map<String,V> header;
    public static HttpEntity create(){
        return new HttpEntity<>();
    }
    public HttpEntity<String,V> builder(String key,V value){
        if (null==this.header) this.header = new HashMap<>();
        this.header.put(key,value);
        return this;
    }
    public HttpEntity<String,V> clean(){
        this.header = new HashMap<>();
        return this;
    }
    public HttpEntity<String,V> clean(String key){
        this.header.remove(key);
        return this;
    }
    public HttpEntity<String,V> clean(String ...keys){
        for (String key : keys) {
            this.header.remove(key);
        }
        return this;
    }
    public Map<String,V> getEntity(){
        return this.header;
    }
}
