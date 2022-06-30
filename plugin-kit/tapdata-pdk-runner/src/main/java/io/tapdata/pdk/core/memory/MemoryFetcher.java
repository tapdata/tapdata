package io.tapdata.pdk.core.memory;

import java.util.List;

public interface MemoryFetcher {
    String MEMORY_LEVEL_SUMMARY = "Summary";
    String MEMORY_LEVEL_IN_DETAIL = "Detail";

    /**
     * Output the memory string for each key.
     *
     * @param mapKeys specified which keys need to output, keys not in mapKeys, no need to output
     * @param memoryLevel output in summary or detail
     * @return MemoryMap
     */
    String memory(List<String> mapKeys, String memoryLevel);
}
