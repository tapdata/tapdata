package io.tapdata.flow.engine.V2.filter;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.util.TapEventUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FilterUtil {

    /**
     * 源端发生新增字段任务会报错，希望能够按照字段编辑节点自动屏蔽新字段，不要影响任务运行
     * tip: 自动屏蔽新字段
     * */
    public static Map<String, Object> processTableFields(Map<String, Object> data, Set<String> fieldNames) {
        if (null == fieldNames || fieldNames.isEmpty()) return data;
        if (null == data) data = new HashMap<>();
        Map<String, Object> finalData = new HashMap<>();
        for (String fieldName : fieldNames) {
            finalData.put(fieldName, data.get(fieldName));
        }
        return finalData;
    }

    public static void filterEventData(TapTable tapTable, TapEvent e) {
        if (null != tapTable && null != tapTable.getNameFieldMap() && !tapTable.getNameFieldMap().isEmpty()) {
            Set<String> fieldNames = tapTable.getNameFieldMap().keySet();
            Map<String, Object> after = TapEventUtil.getAfter(e);
            Map<String, Object> before = TapEventUtil.getBefore(e);
            TapEventUtil.setAfter(e, processTableFields(after, fieldNames));
            TapEventUtil.setBefore(e, processTableFields(before, fieldNames));
        }
    }
}
