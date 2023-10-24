package io.tapdata.entity.verification;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The result for map comparison
 */
public class MapDiff {
    public static MapDiff create() {
        return new MapDiff();
    }

    public static final int RESULT_NOT_MATCH = -1;
    public static final int RESULT_FUZZY_MATCH = 1;
    public static final int RESULT_EXACTLY_MATCH = 100;
    /**
     * final result for the map comparison
     */
    private int result;
    public MapDiff result(int result) {
        this.result = result;
        return this;
    }

    /**
     * different entries for each map key.
     */
    private Map<String, DiffEntry> diffEntriesMap;
    public MapDiff putDiff(String key, DiffEntry diffEntry) {
        if(diffEntriesMap == null) {
            synchronized (this) {
                if(diffEntriesMap == null) {
                    diffEntriesMap = new LinkedHashMap<>();
                }
            }
        }
        diffEntriesMap.put(key, diffEntry);
        return this;
    }

    public DiffEntry deleteDiff(String key) {
        if(diffEntriesMap != null) {
            return diffEntriesMap.remove(key);
        }
        return null;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }

    public Map<String, DiffEntry> getDiffEntriesMap() {
        return diffEntriesMap;
    }
}
