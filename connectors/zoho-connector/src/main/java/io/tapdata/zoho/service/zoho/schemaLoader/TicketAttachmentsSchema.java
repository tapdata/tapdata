package io.tapdata.zoho.service.zoho.schemaLoader;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.service.zoho.loader.TicketAttachmentsOpenApi;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class TicketAttachmentsSchema implements SchemaLoader {
    private static final String TAG = TicketAttachmentsSchema.class.getSimpleName();
    TicketAttachmentsOpenApi attachmentsOpenApi;
    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.attachmentsOpenApi = TicketAttachmentsOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData) {
        return null;
    }

    @Override
    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer) {

    }

    @Override
    public Object timestampToStreamOffset(Long time) {
        return null;
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {

    }

    @Override
    public long batchCount() throws Throwable {
        return 0;
    }
}
