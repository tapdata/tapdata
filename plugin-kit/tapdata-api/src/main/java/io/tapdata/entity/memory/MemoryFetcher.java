package io.tapdata.entity.memory;

import io.tapdata.entity.utils.DataMap;

public interface MemoryFetcher {
    String MEMORY_LEVEL_SUMMARY = "Summary";
    String MEMORY_LEVEL_IN_DETAIL = "Detail";

    /**
     * Output the memory string for each key.
     *
     * @param keyRegex    specified which keys need to output, keys not in mapKeys, no need to output
     * @param memoryLevel output in summary or detail
     * @return MemoryMap
     */
    DataMap memory(String keyRegex, String memoryLevel);
}
