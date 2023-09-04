package io.tapdata.sybase.cdc.dto.analyse.filter;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.sybase.cdc.CdcRoot;

import java.util.List;
import java.util.Set;

public abstract class ReadFilter {
    public static final int LOG_CDC_QUERY_READ_LOG = 0;
    public static final int LOG_CDC_QUERY_READ_SOURCE = 1;

    protected CdcRoot root;

    public static ReadFilter stage(int readType, CdcRoot root) {
        if (readType == LOG_CDC_QUERY_READ_SOURCE) {
            return new ReadSourceFilter().init(root);
        }
        return new ReadLogFilter().init(root);
    }

    public ReadFilter init(CdcRoot root){
        this.root = root;
        return this;
    }


    public List<TapEvent> readFilter(List<TapEvent> events, TapTable tapTable, Set<String> blockFields, String fullTableName) {
        return events;
    }
}
