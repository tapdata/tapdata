package io.tapdata.coding.service.schema;

import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public interface SchemaStart {
    static final String TAG = SchemaStart.class.getSimpleName();
    Set<Class<? extends SchemaStart>> schemaSet = new HashSet<>();

    public Boolean use();

    public String tableName();

    public boolean connection(TapConnectionContext tapConnectionContext);

    public TapTable document(TapConnectionContext connectionContext);

    public default TapTable csv(TapConnectionContext connectionContext) {
        throw new CoreException("May be not support CSV for " + this.tableName() + " Schema.");
    }

    public default Map<String, Object> autoSchema(Map<String, Object> eventData) {
        throw new CoreException("May be not support " + this.tableName() + " to autoSchema.");
    }

    public static SchemaStart getSchemaByName(String schemaName, AtomicReference<String> accessToken) {
        if (Checker.isEmpty(schemaName)) return null;
        Class<?> clz = null;
        try {
            clz = Class.forName("io.tapdata.coding.service.schema" + "." + schemaName);
            Constructor<?> constructor = clz.getConstructor(AtomicReference.class);
            return ((SchemaStart) constructor.newInstance(accessToken));
        } catch (ClassNotFoundException e) {
            TapLogger.debug(TAG, "ClassNotFoundException for Schema {}", schemaName);
        } catch (InstantiationException e1) {
            TapLogger.debug(TAG, "InstantiationException for Schema {}", schemaName);
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e2) {
            TapLogger.debug(TAG, "IllegalAccessException for Schema {}, {}", schemaName, e2.getMessage());
        }
        return null;
    }

    public static List<SchemaStart> getAllSchemas(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        Set<Class<? extends SchemaStart>> allImplClass = new HashSet<>();
        try {
            EnabledSchemas.getAllSchemas(tapConnectionContext, allImplClass, accessToken);
        } catch (Exception e) {
            TapLogger.info(TAG, e.getMessage());
        }
        List<SchemaStart> schemaList = new ArrayList<>();
        allImplClass.forEach(schemaClass -> {
            SchemaStart schema = null;
            try {
                Constructor<? extends SchemaStart> constructor = schemaClass.getConstructor(AtomicReference.class);
                schema = constructor.newInstance(accessToken);
                if (schema.use()) {
                    schemaList.add(schema);
                }
            } catch (InstantiationException e) {
                TapLogger.debug(TAG, "InstantiationException for Schema.");
            } catch (IllegalAccessException e) {
                TapLogger.debug(TAG, "IllegalAccessException for Schema.");
            } catch (NoSuchMethodException | InvocationTargetException methodException) {
                methodException.printStackTrace();
            }
        });
        return schemaList;
    }

    public default Map<String, Object> eventMapToSchemaMap(Map<String, Object> eventData, Map<String, Object> kvMap) {
        Map<String, Object> map = new HashMap<>();
        kvMap.forEach((k, v) -> {
            String subKey = (String) v;
            String[] subKeys = subKey.split("\\.");
            Object subValue = null;
            if (subKeys.length > 0) {
                subValue = eventData.get(subKeys[0]);
                for (int i = 1; subKeys.length > 1 && i < subKeys.length && Checker.isNotEmpty(subValue); i++) {
                    subValue = ((Map<String, Object>) subValue).get(subKeys[i]);
                }
            }
            if (Checker.isNotEmpty(subValue)) {
                map.put(k, subValue);
            }
        });
        return map;
    }
}
