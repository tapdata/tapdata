package io.tapdata.coding.service.connectionMode;

import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;
import java.util.Map;

public interface ConnectionMode {
    public List<TapTable> discoverSchema(List<String> tables, int tableSize);
    public Map<String,Object> attributeAssignment(Map<String,Object> obj);
    public ConnectionMode config(TapConnectionContext connectionContext);
    public static ConnectionMode getInstanceByName(TapConnectionContext connectionContext,String name){
        if (Checker.isEmpty(name)) return null;
        Class clz = null;
        try {
            clz = Class.forName("io.tapdata.coding.service.connectionMode."+name);
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
