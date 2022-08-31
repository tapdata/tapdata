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
    private Map<String, Long> tableUpdateTimeMap;
    public CodingOffset() {
        tableUpdateTimeMap = new HashMap<>();
    }

    public static CodingOffset create(Map<String, Long> tableUpdateTimeMap){
        CodingOffset offset = new CodingOffset();
        offset.setTableUpdateTimeMap(tableUpdateTimeMap);
        return offset;
    }

    public Map<String, Long> getTableUpdateTimeMap() {
        return tableUpdateTimeMap;
    }

    public void setTableUpdateTimeMap(Map<String, Long> tableUpdateTimeMap) {
        this.tableUpdateTimeMap = tableUpdateTimeMap;
    }
}
