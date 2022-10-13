package io.tapdata.zoho.service.zoho.schemaLoader;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.utils.Checker;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface SchemaLoader {
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext);

    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData);

    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer );

    public Object timestampToStreamOffset(Long time);

    public ConnectionOptions connectionTest(Consumer<TestItem> consumer) throws Throwable;

    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) ;

    public long batchCount() throws Throwable ;

    public int tableCount() throws Throwable ;

    public static SchemaLoader loader(String tableName,TapConnectionContext context){
        if (Checker.isEmpty(tableName)) return null;
        Class clz ;
        try {
            clz = Class.forName(SchemaLoader.class.getPackage().getName()+"."+tableName+"Loader");
            return ((SchemaLoader) clz.newInstance()).configSchema(context);
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
