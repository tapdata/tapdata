package io.tapdata.zoho.service.zoho.schemaLoader;

import cn.hutool.core.date.DateUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.zoho.ZoHoConnector;
import io.tapdata.zoho.service.zoho.schema.Schema;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.BeanUtil;
import io.tapdata.zoho.utils.Checker;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface SchemaLoader {
    public boolean isAlive();
    public void init(ZoHoConnector zoHoConnector);
    public void out();
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext);

    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer );

    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) ;

    public long batchCount() throws Throwable ;

    public static SchemaLoader loader(String tableName,TapConnectionContext context,ZoHoConnector zoHoConnector){
        Object bean = BeanUtil.bean(SchemaLoader.class.getPackage().getName() + "." + tableName + "Schema");
        SchemaLoader schema = (SchemaLoader) bean;
        schema.init(zoHoConnector);
        return schema;
    }
    public static List<SchemaLoader> loaders(TapConnectionContext context,ZoHoConnector zoHoConnector){
        List<Schema> schemas = Schemas.allSupportSchemas();
        if ( Checker.isEmpty(schemas) || schemas.isEmpty()) return null;
        Set<SchemaLoader> loaders = new HashSet<>();
        for (Schema schema : schemas) {
            if (Checker.isEmpty(schema)) continue;
            loaders.add(SchemaLoader.loader(schema.schemaName(),context,zoHoConnector));
        }
        if (loaders.isEmpty()) return null;
        return new ArrayList<SchemaLoader>(){{addAll(loaders);}};
    }

}
