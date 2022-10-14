package io.tapdata.zoho.service.zoho.schema;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONNull;
import cn.hutool.json.JSONObject;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.utils.BeanUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface Schema {
    public static Schema schema(String name){
        return BeanUtil.bean("io.tapdata.zoho.service.zoho.schema." + name);
    }
    public List<TapTable> document(List<String> tables, int tableSize );
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext);


    public Map<String,Object> attributeAssignmentDocument(Map<String, Object> obj, TapConnectionContext connectionContext);
    public default Map<String,Object> attributeAssignmentSelfDocument(Map<String, Object> obj){
        this.removeJsonNull(obj);
        return obj;
    }

    public Map<String,Object> attributeAssignmentCsv(Map<String, Object> obj, TapConnectionContext connectionContext, ContextConfig contextConfig);
    public Map<String,Object> attributeAssignmentSelfCsv(Map<String, Object> obj, ContextConfig contextConfig);


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
}
