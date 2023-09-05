package io.tapdata.sybase.cdc.dto.analyse.stream;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.List;

class NormalAccepter implements Accepter {
    private StreamReadConsumer cdcConsumer;

    @Override
    public void accept(String fullTableName, List<TapEvent> events, Object offset) {
        cdcConsumer.accept(events, offset);
    }

    @Override
    public void setStreamReader(StreamReadConsumer cdcConsumer) {
        this.cdcConsumer = cdcConsumer;
    }
}
