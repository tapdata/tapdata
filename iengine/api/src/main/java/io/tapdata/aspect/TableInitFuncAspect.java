package io.tapdata.aspect;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.schema.TapTableMap;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/28 10:30 Create
 */
public class TableInitFuncAspect extends DataFunctionAspect<TableInitFuncAspect> {
    public static final int STATE_PROCESS = 10;

    private TapTableMap<String, TapTable> tapTableMap;

    public TableInitFuncAspect tapTableMap(TapTableMap<String, TapTable> tapTableMap) {
        this.tapTableMap = tapTableMap;
        return this;
    }

    public TapTableMap<String, TapTable> getTapTableMap() {
        return tapTableMap;
    }

    private final Map<String, Boolean> completedMap = new LinkedHashMap<>();

    public TableInitFuncAspect completed(String tableName, Boolean isCreated) {
        completedMap.put(tableName, isCreated);
        return this;
    }

    public Map<String, Boolean> getCompletedMap() {
        return completedMap;
    }

    public long getTotals() {
        return null == tapTableMap ? 0 : tapTableMap.size();
    }

    public long getCompletedCounts() {
        return completedMap.size();
    }
}
