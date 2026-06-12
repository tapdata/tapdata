package com.tapdata.tm.commons.dag.process.duck;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Data
public class JoinInfo {

    private String table;

    private String joinType;

    /**
     * 每组Join条件
     * <p>
     * o.id = d.id
     * o.type = d.type
     */
    private List<JoinKeyPair> joinKeys = new ArrayList<>();

    public static Map<String, Map<String, Object>> toMap(List<JoinInfo> joinKeyInfo) {
        Map<String, Map<String, Object>> joinKeys = new HashMap<>();
        joinKeyInfo.forEach(info -> joinKeys.put(info.getTable(), info.toMap()));
        return joinKeys;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> infoMap = new HashMap<>();
        getJoinKeys().forEach(joinKey -> {
            Optional.ofNullable(joinKey.getLeft()).map(JoinField::toMap).ifPresent(map -> infoMap.put("left", map));
            Optional.ofNullable(joinKey.getRight()).map(JoinField::toMap).ifPresent(map -> infoMap.put("right", map));
        });
        return infoMap;
    }
}