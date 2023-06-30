package io.tapdata.zoho.entity;

import java.util.HashMap;
import java.util.Map;

public class HttpEntity<K,V>{
    Map<K,V> entity;
    private HttpEntity(){
        this.entity = new HashMap<>();
    }
    public static HttpEntity create(){
        return new HttpEntity();
    }
    public static HttpEntity create(Map entity){
        return new HttpEntity().addAll(entity);
    }

    public HttpEntity addAll(Map<K,V> entity){
        this.entity.putAll(entity);
        return this;
    }
    public HttpEntity build(K key,V value){
        this.entity.put(key,value);
        return this;
    }

    public HttpEntity remove(K key){
        this.entity.remove(key);
        return this;
    }

    public HttpEntity build(Map.Entry<K,V> entry){
        this.entity.put(entry.getKey(),entry.getValue());
        return this;
    }
    public Map<K,V> entity(){
        return this.entity;
    }
    public V get(K key){
        return this.entity.get(key);
    }

    public Map<K, V> getEntity() {
        return entity;
    }

    public void setEntity(Map<K, V> entity) {
        this.entity = entity;
    }
}