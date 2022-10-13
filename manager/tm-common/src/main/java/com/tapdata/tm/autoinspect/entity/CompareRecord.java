package com.tapdata.tm.autoinspect.entity;

import com.tapdata.tm.autoinspect.constants.CompareStatus;
import lombok.Data;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.text.SimpleDateFormat;
import java.util.*;

@Data
public class CompareRecord {

    private @NonNull String tableName;
    private @NonNull ObjectId connectionId;
    private @NonNull LinkedHashMap<String, Object> originalKey;
    private @NonNull LinkedHashSet<String> keyNames;
    private Map<String, Object> data;

    public CompareRecord() {
        this.originalKey = new LinkedHashMap<>();
        this.keyNames = new LinkedHashSet<>();
    }

    public CompareRecord(@NonNull String tableName, @NonNull ObjectId connectionId) {
        this();
        this.tableName = tableName;
        this.connectionId = connectionId;
    }

    public CompareRecord(@NonNull String tableName, @NonNull ObjectId connectionId, @NonNull LinkedHashMap<String, Object> originalKey, @NonNull LinkedHashSet<String> keyNames, Map<String, Object> data) {
        this.tableName = tableName;
        this.connectionId = connectionId;
        this.originalKey = originalKey;
        this.keyNames = keyNames;
        this.data = data;
    }

    public Object getDataValue(@NonNull String key) {
        return data.get(key);
    }

    /**
     * Calculate primary key order
     *
     * @param o target value
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

            if (v1 instanceof Comparable && v2 instanceof Comparable) {
                compareValue = ((Comparable) v1).compareTo(v2);
            } else {
                compareValue = v1.hashCode() - v2.hashCode();
            }
            if (0 < compareValue) {
                return CompareStatus.MoveTarget;
            } else if (0 > compareValue) {
                return CompareStatus.MoveSource;
            }
        }

        return CompareStatus.Diff;
    }

    public CompareStatus compare(CompareRecord targetData) {
        CompareStatus compareStatus = this.compareKeys(targetData);
        if (CompareStatus.Diff == compareStatus) {
            Object odata, ndata;
            List<String> diffKeys = new ArrayList<>();
            Map<String, Object> omap = targetData.getData();
            for (String key : this.getData().keySet()) {
                //check null value
                odata = omap.compute(key, this::parse);
                ndata = this.getData().compute(key, this::parse);

                //filter keys
                if (this.getKeyNames().contains(key)) continue;

                if (null == ndata) {
                    if (null != odata) {
                        diffKeys.add(key);
                    }
                    continue;
                }

                //check value
                if (!ndata.equals(odata)) {
                    diffKeys.add(key);
                }
            }

            //has not difference
            if (diffKeys.isEmpty()) {
                compareStatus = CompareStatus.Ok;
            }
        }

        return compareStatus;
    }

    public Object parse(String k, Object v) {
        if (null == v) return null;

        if (v instanceof String) {
            return v;
        } else if (v instanceof ObjectId) {
            return ((ObjectId) v).toHexString();
        } else if (v instanceof Date) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM_dd HH:mm:ss.S");
            return sdf.format(v);
        }
        return v.toString();
    }
}
