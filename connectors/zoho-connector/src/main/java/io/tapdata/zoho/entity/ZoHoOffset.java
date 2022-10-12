package io.tapdata.zoho.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * ZoHo offset.
 *
 * @author <a href="https://github.com/11000100111010101100111">GavinXiao</a>
 * @version v1.0 2022/9/26 14:06 Create
 */
public class ZoHoOffset {
    private Map<String, Long> tableUpdateTimeMap;
    public ZoHoOffset() {
        tableUpdateTimeMap = new HashMap<>();
    }

    public static ZoHoOffset create(Map<String, Long> tableUpdateTimeMap){
        ZoHoOffset offset = new ZoHoOffset();
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