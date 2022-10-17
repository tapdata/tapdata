package io.tapdata.zoho.service.zoho.schemaLoader;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.zoho.utils.BeanUtil;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface SchemaLoader {
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext);

    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData);

    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer );

    public Object timestampToStreamOffset(Long time);

    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) ;

    public long batchCount() throws Throwable ;

    public static SchemaLoader loader(String tableName,TapConnectionContext context){
        return  BeanUtil.bean(SchemaLoader.class.getPackage().getName()+"."+tableName+"Loader");
    }
}
