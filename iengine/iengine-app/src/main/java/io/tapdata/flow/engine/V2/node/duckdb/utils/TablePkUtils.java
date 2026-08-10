package io.tapdata.flow.engine.V2.node.duckdb.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/16 15:13 Create
 * @description
 */
public class TablePkUtils {

    private TablePkUtils() {}

    public static List<Map<String, Object>> pkValues(List<Map<String, Object>> rows, List<String> pks) {
        if (CollectionUtils.isEmpty(pks) || CollectionUtils.isEmpty(rows)) {
            return new ArrayList<>();
        }
        return rows.stream()
                .map(row -> TablePkUtils.pkValue(row, pks))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public static Map<String, Object> pkValue(Map<String, Object> row, List<String> pks) {
        if (CollectionUtils.isEmpty(pks)) {
            return null;
        }
        if (MapUtils.isEmpty(row)) {
            return null;
        }
        Map<String, Object> pkValue = new HashMap<>();
        pks.forEach(pk -> pkValue.put(pk, row.get(pk)));
        return pkValue;
    }

    public static boolean pkExists(List<Map<String, Object>> afterPks, Map<String, Object> rowPkValue) {
        if (CollectionUtils.isEmpty(afterPks) || MapUtils.isEmpty(rowPkValue)) {
            return false;
        }
        for (Map<String, Object> afterPk : afterPks) {
            if (MapUtils.isEmpty(afterPk)) {
                continue;
            }
            if (afterPk.size() != rowPkValue.size()) {
                continue;
            }
            if (!afterPk.keySet().equals(rowPkValue.keySet())) {
                continue;
            }
            boolean matched = true;
            for (String key : rowPkValue.keySet()) {
                if (!Objects.equals(afterPk.get(key), rowPkValue.get(key))) {
                    matched = false;
                    break;
                }
            }
            if (matched) {
                return true;
            }
        }
        return false;
    }
}
