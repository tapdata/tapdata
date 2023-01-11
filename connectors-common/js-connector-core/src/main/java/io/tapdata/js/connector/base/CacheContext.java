package io.tapdata.js.connector.base;

import java.util.HashMap;
import java.util.Map;

public class CacheContext {
    private Map<String ,CacheData> data = new HashMap<>();

    public Object get(String key){

        return null;
    }

    public Object save(String key, Object data, long lifeCycle){

        return data;
    }
    public Object save(String key, Object data){

        return data;
    }

    private class CacheData{
        private long saveTime;
        private long  lifeCycle;
        private String key;
        private Object data;
    }
}
