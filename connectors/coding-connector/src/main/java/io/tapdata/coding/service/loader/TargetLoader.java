package io.tapdata.coding.service.loader;

import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface TargetLoader {
    static final String TAG = TargetLoader.class.getSimpleName();

    public static TargetLoader loader(TapConnectionContext tapConnectionContext, String tableName){
        Class clz = null;
        try {
            clz = Class.forName( TargetLoader.class.getPackage().getName()+"." + tableName+"Loader");
            Constructor com = clz.getConstructor(TapConnectionContext.class);
            return (TargetLoader)com.newInstance(tapConnectionContext);
        } catch (ClassNotFoundException e0) {
            TapLogger.debug(TAG, "ClassNotFoundException for Schema {}",tableName);
        }catch (NoSuchMethodException e1) {
            TapLogger.debug(TAG, "NoSuchMethodException for Schema {}",tableName);
        } catch (InstantiationException e2) {
            TapLogger.debug(TAG, "InstantiationException for Schema {}",tableName);
        } catch (IllegalAccessException e3) {
            TapLogger.debug(TAG, "IllegalAccessException for Schema {}",tableName);
        } catch (InvocationTargetException e4) {
            TapLogger.debug(TAG, "InvocationTargetException for Schema {}", tableName);
        }
        return null;
    }

    public static List<TargetLoader> loader(TapConnectionContext tapConnectionContext, List<String> tableName){
        List<TargetLoader> loaders = new ArrayList<>();
        if (Checker.isEmpty(tableName)) return loaders;
        for (String table : tableName) {
            TargetLoader loader = TargetLoader.loader(tapConnectionContext,table);
            if (Checker.isNotEmpty(loader)){
                loaders.add(loader);
            }
        }
        return loaders;
    }

    public void writeRecord(
            List<TapRecordEvent> tapRecordEvents,
            Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) ;
}
