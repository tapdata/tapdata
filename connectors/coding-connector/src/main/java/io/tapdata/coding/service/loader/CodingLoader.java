package io.tapdata.coding.service.loader;

import io.tapdata.coding.CodingConnector;
import io.tapdata.coding.entity.param.Param;
import io.tapdata.coding.enums.CodingEvent;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public interface CodingLoader<T extends Param> {

    static final String TAG = CodingLoader.class.getSimpleName();

    public Long streamReadTime();

    public CodingStarter connectorInit(CodingConnector codingConnector);

    public CodingStarter connectorOut();

    public static CodingLoader<Param> loader(TapConnectionContext tapConnectionContext, String tableName, AtomicReference<String> accessToken) {
        Class<?> clz = null;
        try {
            clz = Class.forName("io.tapdata.coding.service.loader." + tableName + "Loader");//CodingLoader.class.getPackage().getName()
            Constructor<?> com = clz.getConstructor(TapConnectionContext.class, AtomicReference.class);
            return (CodingLoader) com.newInstance(tapConnectionContext, accessToken);
        } catch (ClassNotFoundException e0) {
            TapLogger.debug(TAG, "ClassNotFoundException for Schema {}", tableName);
        } catch (NoSuchMethodException e1) {
            TapLogger.debug(TAG, "NoSuchMethodException for Schema {}", tableName);
        } catch (InstantiationException e2) {
            TapLogger.debug(TAG, "InstantiationException for Schema {}", tableName);
        } catch (IllegalAccessException e3) {
            TapLogger.debug(TAG, "IllegalAccessException for Schema {}", tableName);
        } catch (InvocationTargetException e4) {
            TapLogger.debug(TAG, "InvocationTargetException for Schema {}", tableName);
        }
        return null;
    }

    public static List<CodingLoader<Param>> loader(TapConnectionContext tapConnectionContext, List<String> tableName, AtomicReference<String> accessToken) {
        List<CodingLoader<Param>> loaders = new ArrayList<>();
        if (Checker.isEmpty(tableName)) return loaders;
        for (String table : tableName) {
            CodingLoader<Param> loader = CodingLoader.loader(tapConnectionContext, table, accessToken);
            if (Checker.isNotEmpty(loader)) {
                loaders.add(loader);
            }
        }
        return loaders;
    }

    public List<Map<String, Object>> list(T param);

    public List<Map<String, Object>> all(T param);

    public CodingHttp codingHttp(T param);

    public default Map<String, Object> get(T param) {
        return null;
    }

    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer);

    public int batchCount() throws Throwable;

    public void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer);

    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData);

    public default CodingEvent getRowDataCallBackEvent(Map<String, Object> eventData) {
        if (Checker.isEmpty(eventData)) {
//            throw new CoreException("Row data call back event data is empty.");
            TapLogger.warn(TAG, "Row data call back event data is empty. will be ignored. " + eventData);
            return null;
        }
        Object event = eventData.get("event");
        if (Checker.isEmpty(event)) {
//            throw new CoreException("Row data call back event type is empty.");
            TapLogger.warn(TAG, "Row data call back event type is empty. will be ignored. " + eventData);
            return null;
        }
        String webHookEventType = String.valueOf(event);
        return CodingEvent.event(webHookEventType);
    }

    public default String longToDateStr(Long date) {
        if (null == date) return "1000-01-01";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = formatter.format(new Date(date));
        return dateStr.length() > 10 ? "9999-12-31" : dateStr;
    }

    public default String longToDateTimeStr(Long date) {
        if (null == date) return "1000-01-01 00:00:00";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String dateStr = formatter.format(new Date(date));
        return dateStr.length() > 19 ? "9999-12-31 23:59:59" : dateStr;
    }
}
