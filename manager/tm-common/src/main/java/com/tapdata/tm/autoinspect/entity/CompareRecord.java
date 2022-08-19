package com.tapdata.tm.autoinspect.entity;

import com.tapdata.tm.autoinspect.constants.CompareStatus;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

@Setter
@Getter
public class CompareRecord {

    private @NonNull String tableName;
    private @NonNull ObjectId connectionId;
    private @NonNull LinkedHashMap<String, Object> originalKey;
    private @NonNull LinkedHashSet<String> keyNames;
    private @NonNull Map<String, Object> data;

    public CompareRecord() {
        this.originalKey = new LinkedHashMap<>();
        this.keyNames = new LinkedHashSet<>();
        this.data = new HashMap<>();
    }

    public CompareRecord(@NonNull String tableName, @NonNull ObjectId connectionId, @NonNull LinkedHashMap<String, Object> originalKey, @NonNull LinkedHashSet<String> keyNames, @NonNull Map<String, Object> data) {
        this.tableName = tableName;
        this.connectionId = connectionId;
        this.originalKey = originalKey;
        this.keyNames = keyNames;
        this.data = data;
    }

    public Object getDataValue(@NonNull String key) {
        return data.get(key);
    }

    public void copyFrom(@NonNull CompareRecord o) {
        this.originalKey.putAll(o.getOriginalKey());
        this.keyNames.addAll(o.getKeyNames());
        this.data.putAll(o.getData());
    }

    /**
     * Calculate primary key order
     *
     * @param o    target value
     * @return
     */
    public CompareStatus compareKeys(@NonNull CompareRecord o) {
        int compareValue;
        Object v1, v2;
        for (String k : getKeyNames()) {
            v1 = getDataValue(k);
            v2 = o.getDataValue(k);
            if (null == v1) {
                if (null == v2) continue;
                return CompareStatus.MoveSource;
            } else if (null == v2) {
                return CompareStatus.MoveTarget;
            }

            compareValue = v1.hashCode() - v2.hashCode();
            if (0 < compareValue) {
                return CompareStatus.MoveTarget;
            } else if (0 > compareValue) {
                return CompareStatus.MoveSource;
            }
        }

        return CompareStatus.Diff;
    }
}
