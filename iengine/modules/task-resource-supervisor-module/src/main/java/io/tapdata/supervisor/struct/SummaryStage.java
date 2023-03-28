package io.tapdata.supervisor.struct;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;

public interface SummaryStage extends MemoryFetcher {
    public default DataMap getSummary(){

        return null;
    }
}
