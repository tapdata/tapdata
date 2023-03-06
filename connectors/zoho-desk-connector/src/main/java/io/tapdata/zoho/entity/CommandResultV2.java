package io.tapdata.zoho.entity;

import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.zoho.utils.MapUtil;

import java.util.HashMap;
import java.util.Map;

public class CommandResultV2 extends CommandResult {
    final static String SET_VALUE_KEY_NAME = "setValue";
    final static String DATA_NAME_KEY = "data";
    public static CommandResultV2 create(Map<String, Object> result){
        return new CommandResultV2(result);
    }
    CommandResultV2(Map<String, Object> result){
        super();
        super.result(result);
    }
    public CommandResultV2 setValue(String key){
        Map<String, Object> result = this.getResult();
        if (null == result ) result = new HashMap<>();
        MapUtil.asMapPutOntoMap(
                MapUtil.DOT,
                result,
                (Map<String, Object>) result.computeIfAbsent(SET_VALUE_KEY_NAME, map -> new HashMap<String,Object>()),
                key,
                DATA_NAME_KEY);
        return this;
    }
    public CommandResultV2 onlyGetSetValue(){
        Map<String, Object> result = this.getResult();
        if (null == result ) result = new HashMap<>();
        return CommandResultV2.create((Map<String, Object>) result.computeIfAbsent(SET_VALUE_KEY_NAME, map -> new HashMap<String,Object>()));
    }
}
