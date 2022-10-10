package io.tapdata.coding.service.loader;

import io.tapdata.coding.entity.param.Param;
import io.tapdata.coding.enums.CodingEvent;
import io.tapdata.coding.service.schema.SchemaStart;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public interface CodingLoader<T extends Param> {
    public Long streamReadTime();

    public static CodingLoader<Param> loader(TapConnectionContext tapConnectionContext, String tableName){
        Class clz = null;
        try {
            clz = Class.forName(CodingLoader.class.getPackage().getName() + "." + tableName+"Loader");
            Constructor com = clz.getConstructor(TapConnectionContext.class);
            return (CodingLoader)com.newInstance(tapConnectionContext);
        } catch (ClassNotFoundException e0) {
            //e0.printStackTrace();
        }catch (NoSuchMethodException e1) {
            //e1.printStackTrace();
        } catch (InstantiationException e2) {
            //e2.printStackTrace();
        } catch (IllegalAccessException e3) {
            //e3.printStackTrace();
        } catch (InvocationTargetException e4) {
            //e4.printStackTrace();
        }
        return null;
    }

    public static List<CodingLoader<Param>> loader(TapConnectionContext tapConnectionContext, List<String> tableName){
        List<CodingLoader<Param>> loaders = new ArrayList<>();
        if (Checker.isEmpty(tableName)) return loaders;
        for (String table : tableName) {
            CodingLoader<Param> loader = CodingLoader.loader(tapConnectionContext,table);
            if (Checker.isNotEmpty(loader)){
                loaders.add(loader);
            }
        }
        return loaders;
    }

    public List<Map<String,Object>> list(T param);

    public List<Map<String,Object>> all(T param);

    public CodingHttp codingHttp(T param);

    public default Map<String,Object> get(T param){
        return null;
    }

    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer);

    public long batchCount() throws Throwable;

    public void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer);

    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData);

    public default CodingEvent getRowDataCallBackEvent(Map<String, Object> eventData){
        if (Checker.isEmpty(eventData)) return null;
        Object event = eventData.get("event");
        if (Checker.isEmpty(event)) return null;
        String webHookEventType = String.valueOf(event);
        return CodingEvent.event(webHookEventType);
    }

    public default String longToDateStr(Long date){
        if (null == date) return "1000-01-01";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = formatter.format(new Date(date));
        return dateStr.length()>10?"9999-12-31":dateStr;
    }
}
