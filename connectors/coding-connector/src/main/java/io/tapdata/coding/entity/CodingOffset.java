package io.tapdata.coding.entity;

import java.util.HashMap;
import java.util.Map;
/**
 * Coding offset.
 *
 * @author <a href="https://github.com/11000100111010101100111">GavinXiao</a>
 * @version v1.0 2022/8/23 16:37 Create
 */
public class CodingOffset {
    private Map<String, String> tableUpdateTimeMap;
    public CodingOffset() {
        tableUpdateTimeMap = new HashMap<>();
    }

    public Map<String, String> getTableUpdateTimeMap() {
        return tableUpdateTimeMap;
    }

    public void setTableUpdateTimeMap(Map<String, String> tableUpdateTimeMap) {
        this.tableUpdateTimeMap = tableUpdateTimeMap;
    }
}
