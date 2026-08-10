package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.dag.process.DuckDbSqlNode;
import com.tapdata.tm.commons.dag.process.FromTableConfig;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/26 11:10 Create
 * @description
 */
public final class DuckNodeHandler {
    private DuckNodeHandler() {

    }

    public static void copy(DuckDbSqlNode sqlNode, Map<String, String> oldnewNodeIdMap) {
        List<TapTableDto> preNodeTapTables = sqlNode.getPreNodeTapTables();
        if (preNodeTapTables != null && !preNodeTapTables.isEmpty()) {
            for (TapTableDto preNodeTapTable : preNodeTapTables) {
                String oldNodeId = preNodeTapTable.getId();
                String newId = oldnewNodeIdMap.get(oldNodeId);
                preNodeTapTable.setId(newId);
            }
        }
        List<FromTableConfig> fromTables = sqlNode.getFromTables();
        if (fromTables != null && !fromTables.isEmpty()) {
            fromTables.forEach(fromTable -> {
                String preNodeId = fromTable.getPreNodeId();
                String newId = oldnewNodeIdMap.get(preNodeId);
                fromTable.setPreNodeId(newId);
            });
        }
    }
}
