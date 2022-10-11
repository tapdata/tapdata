package io.tapdata.coding.service.schema;

import io.tapdata.coding.utils.beanUtil.BeanUtil;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.reflections.Reflections;

import java.util.*;

public interface SchemaStart {
    public Boolean use();
    public String tableName();

    public TapTable document(TapConnectionContext connectionContext);

    public TapTable csv(TapConnectionContext connectionContext);

    public Map<String,Object> autoSchema(Map<String,Object> eventData);

    public static SchemaStart getSchemaByName(String schemaName){
        if (Checker.isEmpty(schemaName)) return null;
        Class clz = null;
        try {
            clz = Class.forName(SchemaStart.class.getPackage().getName() + "."+schemaName);
            return ((SchemaStart)clz.newInstance());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e1) {
            e1.printStackTrace();
        } catch (IllegalAccessException e2) {
            e2.printStackTrace();
        }
        return null;
    }

    public static List<SchemaStart> getAllSchemas(){
        Reflections reflections = new Reflections(SchemaStart.class.getPackage().getName());
        Set<Class<? extends SchemaStart>> allImplClass = reflections.getSubTypesOf(SchemaStart.class);
        List<SchemaStart> schemaList = new ArrayList<>();
        allImplClass.forEach(schemaClass->{
            SchemaStart schema = null;
            try {
                schema = schemaClass.newInstance();
                if (schema.use()){
                    schemaList.add(schema);
                }
            } catch (InstantiationException e) {

            } catch (IllegalAccessException e) {

            }
        });
        return schemaList;
    }

    public default Map<String,Object> eventMapToSchemaMap(Map<String,Object> eventData,Map<String,Object> kvMap){
            Map<String,Object> map = new HashMap<>();
            kvMap.forEach((k,v)->{
                String subKey = (String)v;
                String[] subKeys = subKey.split("\\.");
                Object subValue = null;
                if (subKeys.length>0) {
                    subValue = eventData.get(subKeys[0]);
                    for (int i = 1; subKeys.length > 1 && i < subKeys.length && Checker.isNotEmpty(subValue); i++) {
                        subValue = ( (Map<String,Object>)subValue).get(subKeys[i]);
                    }
                }
                if (Checker.isNotEmpty(subValue)) {
                    map.put(k, subValue);
                }
            });
            return map;
        }
}
