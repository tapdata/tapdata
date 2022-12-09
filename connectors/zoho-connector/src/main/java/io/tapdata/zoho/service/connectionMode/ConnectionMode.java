package io.tapdata.zoho.service.connectionMode;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONNull;
import cn.hutool.json.JSONObject;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface ConnectionMode {
    public List<TapTable> discoverSchema(List<String> tables, int tableSize);
    public List<TapTable> discoverSchemaV1(List<String> tables, int tableSize);

    public Map<String,Object> attributeAssignment(Map<String, Object> obj,String tableName, ZoHoBase openApi);
    public Map<String,Object> attributeAssignmentSelf(Map<String, Object> obj,String tableName);

    public ConnectionMode config(TapConnectionContext connectionContext);
    public default void removeJsonNull(Map<String, Object> map){
        if (null == map || map.isEmpty()) return;
        Iterator<Map.Entry<String,Object>> iteratorMap = map.entrySet().iterator();
        while (iteratorMap.hasNext()){
            Map.Entry<String,Object> entry = iteratorMap.next();
            Object value = entry.getValue();
            if (value instanceof JSONNull){
                iteratorMap.remove();
            }
            else if ((value instanceof JSONObject) || (value instanceof Map)){
                removeJsonNull((Map<String,Object>)value);
            }else if(value instanceof JSONArray || value instanceof List){
                try {
                    List<Map<String, Object>> list = (List<Map<String, Object>>) value;
                    list.forEach(item->removeJsonNull(item));
                }catch (Exception e){
                }
            }
        }
    }
    public static ConnectionMode getInstanceByName(TapConnectionContext connectionContext, String name){
        if (Checker.isEmpty(name)) return null;
        Class clz = null;
        try {
            clz = Class.forName("io.tapdata.zoho.service.connectionMode.impl."+name);
            return ((ConnectionMode)clz.newInstance()).config(connectionContext);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e1) {
            e1.printStackTrace();
        } catch (IllegalAccessException e2) {
            e2.printStackTrace();
        }
        return null;
    }
}
