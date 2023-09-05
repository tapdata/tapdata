package io.tapdata.sybase.cdc.dto.analyse.stream;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.dto.analyse.filter.ReadFilter;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Accepter {
    public static Accepter create(Integer type, StreamReadConsumer cdcConsumer, ReadFilter readFilter, CdcRoot root, Map<String, Set<String>> blockFieldsMap, Map<String, TapTable> tapTableMap) {
        Accepter accepter = null == type || type != ReadFilter.LOG_CDC_QUERY_READ_SOURCE ?
                new NormalAccepter() : new TextAccepter(blockFieldsMap, tapTableMap);
        accepter.setFilter(readFilter);
        accepter.setRoot(root);
        accepter.setStreamReader(cdcConsumer);
        return accepter;
    }

    public void accept(String fullTableName, List<TapEvent> events, Object offset);

    public void setStreamReader(StreamReadConsumer cdcConsumer);

    public default void setFilter(ReadFilter readFilter){}

    public default void setRoot(CdcRoot root){}

    public default void close(){}
}
