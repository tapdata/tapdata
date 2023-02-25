package io.tapdata.zoho.service.zoho.schema;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONNull;
import cn.hutool.json.JSONObject;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.service.zoho.loader.ProductsOpenApi;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.BeanUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 策略接口，根据表名称生成表
 * 通过Schema.allSupportSchemas()获取所有支持的表
 * */
public interface Schema {
    public Schema config(ZoHoBase openApi);
    public static Schema schema(String name){
        return BeanUtil.bean("io.tapdata.zoho.service.zoho.schema." + name);
    }

    public String schemaName();
    /**
     * 文档类型输出表结构
     * */
    public List<TapTable> document(List<String> tables, int tableSize );

    /**
     * CSV类型输出表结构
     * */
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext);

    /**
     * 文档类型数据获取：此接口方法为---需要根据主键请求一次详情信息，如果Map为详情信息就不需要请求详情了
     * */
    public default Map<String,Object> attributeAssignmentDocument(Map<String, Object> obj, TapConnectionContext connectionContext){
        return this.attributeAssignmentSelfDocument(this.getDetail(obj, connectionContext));
    }

    /**
     * 文档类型数据获取：此接口方法为---直接使用Map根据需要处理成表数据，不需要去获取详情
     * */
    public default Map<String,Object> attributeAssignmentSelfDocument(Map<String, Object> obj){
        this.removeJsonNull(obj);
        return obj;
    }

    /**
     * CSV类型数据获取：此接口方法为---需要根据主键请求一次详情信息，如果Map为详情信息就不需要请求详情了
     * */
    public default Map<String,Object> attributeAssignmentCsv(Map<String, Object> obj, TapConnectionContext connectionContext, ContextConfig contextConfig){
        return this.attributeAssignmentSelfCsv(this.getDetail(obj,connectionContext),contextConfig);
    }

    /**
     * CSV类型数据获取：此接口方法为---直接使用Map根据需要处理成表数据，不需要去获取详情
     * */
    public Map<String,Object> attributeAssignmentSelfCsv(Map<String, Object> obj, ContextConfig contextConfig);

    /**
     * 移除Map中值为null的键值
     * */
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

    public Map<String,Object> getDetail(Map<String,Object> map, TapConnectionContext connectionContext);
}
