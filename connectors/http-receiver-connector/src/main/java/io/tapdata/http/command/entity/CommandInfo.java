package io.tapdata.http.command.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * @author GavinXiao
 * @description CommandInfo create by Gavin
 * @create 2023/5/26 10:01
 **/
public class CommandInfo {
    Map<String, Object> config;

    public static CommandInfo create(){
        return new CommandInfo();
    }

    public CommandInfo config(String key, Object info){
        if (null == config) config = new HashMap<>();
        config.put(key, info);
        return this;
    }

    public Object config(String key){
        if (null == config) return null;
        return config.get(key);
    }
}
