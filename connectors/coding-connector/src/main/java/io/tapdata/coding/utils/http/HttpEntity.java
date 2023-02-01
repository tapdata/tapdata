package io.tapdata.coding.utils.http;

import java.util.HashMap;
import java.util.Map;

public class HttpEntity<String, V> {
    private Map<String, V> entity;

    public static HttpEntity create() {
        return new HttpEntity<>();
    }

    public HttpEntity<String, V> builder(String key, V value) {
        if (null == this.entity) this.entity = new HashMap<>();
        this.entity.put(key, value);
        return this;
    }

    public HttpEntity<String, V> builderIfNotAbsent(String key, V value) {
        if (null == this.entity) this.entity = new HashMap<>();
        if (null == value) return this;
        if (value instanceof Map && ((Map<String, V>) value).isEmpty()) return this;
        this.entity.put(key, value);
        return this;
    }

    public HttpEntity<String, V> clean() {
        this.entity = new HashMap<>();
        return this;
    }

    public HttpEntity<String, V> clean(String key) {
        this.entity.remove(key);
        return this;
    }

    public HttpEntity<String, V> clean(String... keys) {
        for (String key : keys) {
            this.entity.remove(key);
        }
        return this;
    }

    public Map<String, V> getEntity() {
        return this.entity;
    }
}
