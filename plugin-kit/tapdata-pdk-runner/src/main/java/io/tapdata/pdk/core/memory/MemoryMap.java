package io.tapdata.pdk.core.memory;

import java.util.LinkedHashMap;

public class MemoryMap extends LinkedHashMap<String, String> {
    public static final String DEFAULT_KEY = "_memory_default";
    public static MemoryMap create(String memory) {
        MemoryMap memoryMap = new MemoryMap();
        memoryMap.put(DEFAULT_KEY, memory);
        return memoryMap;
    }
    public static MemoryMap create() {
        return new MemoryMap();
    }

}
