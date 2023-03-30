package io.tapdata.coding.service.connectionMode;

import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public interface ConnectionMode {
    static final String TAG = ConnectionMode.class.getSimpleName();

    public List<TapTable> discoverSchema(List<String> tables, int tableSize, AtomicReference<String> accessToken);

    public Map<String, Object> attributeAssignment(Map<String, Object> obj);

    public ConnectionMode config(TapConnectionContext connectionContext, AtomicReference<String> accessToken);

    public static ConnectionMode getInstanceByName(TapConnectionContext connectionContext, AtomicReference<String> accessToken, String name) {
        if (Checker.isEmpty(name)) return null;
        Class<?> clz = null;
        try {
            clz = Class.forName("io.tapdata.coding.service.connectionMode." + name);//ConnectionMode.class.getPackage().getName()
            return ((ConnectionMode) clz.newInstance()).config(connectionContext, accessToken);
        } catch (ClassNotFoundException e) {
            TapLogger.debug(TAG, "ClassNotFoundException for connectionMode {}", name);
        } catch (InstantiationException e1) {
            TapLogger.debug(TAG, "InstantiationException for connectionMode {}", name);
        } catch (IllegalAccessException e2) {
            TapLogger.debug(TAG, "IllegalAccessException for connectionMode {}", name);
        }
        return null;
    }
}
