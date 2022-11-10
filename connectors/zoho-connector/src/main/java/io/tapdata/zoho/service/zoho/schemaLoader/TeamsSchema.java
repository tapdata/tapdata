package io.tapdata.zoho.service.zoho.schemaLoader;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TeamsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class TeamsSchema extends Schema implements SchemaLoader {

    private static final String TAG = TeamsSchema.class.getSimpleName();
    TeamsOpenApi teamsOpenApi;
    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.teamsOpenApi = TeamsOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer) {

    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        this.read(batchCount,offset,consumer,Boolean.TRUE);
    }

    @Override
    public long batchCount() throws Throwable {
        return 0;
    }

    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer,boolean isBatchRead ){
        TapConnectionContext context = this.teamsOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString("connectionMode");
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        if (null == connectionMode) throw new CoreException("Connection Mode is not empty or not null.");
        String tableName =  Schemas.Products.getTableName();
        List<Map<String, Object>> list = teamsOpenApi.page();
        if (Checker.isEmpty(list) || list.isEmpty()) return;
        List<TapEvent> events = new ArrayList<>();
        for (Map<String, Object> product : list) {
            if (!isAlive()) break;
            Map<String, Object> oneProduct = connectionMode.attributeAssignment(product,tableName,teamsOpenApi);
            if (Checker.isEmpty(oneProduct) || oneProduct.isEmpty()) continue;
            events.add(( TapSimplify.insertRecordEvent(oneProduct, tableName).referenceTime(System.currentTimeMillis()) ));
            if (events.size() != readSize) continue;
            consumer.accept(events, offsetState);
            events = new ArrayList<>();
        }
        if (events.isEmpty()) return;
        consumer.accept(events, offsetState);
    }
}
