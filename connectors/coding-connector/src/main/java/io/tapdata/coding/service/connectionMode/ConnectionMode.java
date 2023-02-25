package io.tapdata.coding.service.connectionMode;

import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.List;
import java.util.Map;

public interface ConnectionMode {
    static final String TAG = ConnectionMode.class.getSimpleName();
    public List<TapTable> discoverSchema(List<String> tables, int tableSize);
    public Map<String,Object> attributeAssignment(Map<String,Object> obj);
    public ConnectionMode config(TapConnectionContext connectionContext);
    public static ConnectionMode getInstanceByName(TapConnectionContext connectionContext,String name){
        if (Checker.isEmpty(name)) return null;
        Class clz = null;
        try {
            clz = Class.forName( "io.tapdata.coding.service.connectionMode." + name);//ConnectionMode.class.getPackage().getName()
            return ((ConnectionMode)clz.newInstance()).config(connectionContext);
        } catch (ClassNotFoundException e) {
            TapLogger.debug(TAG, "ClassNotFoundException for connectionMode {}",name);
        } catch (InstantiationException e1) {
            TapLogger.debug(TAG, "InstantiationException for connectionMode {}",name);
        } catch (IllegalAccessException e2) {
            TapLogger.debug(TAG, "IllegalAccessException for connectionMode {}",name);
        }
        return null;
    }
}
