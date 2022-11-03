package com.tapdata.tm.autoinspect.entity;

import cn.hutool.crypto.digest.MD5;
import com.tapdata.tm.autoinspect.constants.CompareStatus;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapYear;
import io.tapdata.entity.schema.value.DateTime;
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

    public CompareRecord(@NonNull String tableName, @NonNull ObjectId connectionId, @NonNull LinkedHashMap<String, Object> originalKey, @NonNull LinkedHashSet<String> keyNames) {
        this.tableName = tableName;
        this.connectionId = connectionId;
        this.originalKey = originalKey;
        this.keyNames = keyNames;
    }

    public void setData(Map<String, Object> data) {
        if (null == data) {
            this.data = null;
            return;
        }

        this.data = new LinkedHashMap<>();
        this.data.putAll(data);
    }

    public void setData(Map<String, Object> data, Map<String, TapField> tapFieldMap) {
        if (null == data) {
            this.data = null;
            return;
        }

        this.data = new LinkedHashMap<>();
        for (Map.Entry<String, Object> en : data.entrySet()) {
            this.data.put(en.getKey(), parse(tapFieldMap.get(en.getKey()), en.getKey(), en.getValue()));
        }
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

            if (v1 instanceof Comparable && v1.getClass().equals(v2.getClass())) {
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
                odata = omap.get(key);
                ndata = this.getDataValue(key);

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

    public Object parse(TapField field, String k, Object v) {
        if (null == v) return null;

        if (v instanceof DateTime) {
            if (field.getTapType() instanceof TapTime) {
                return ((DateTime)v).toTime().toString();
            } else if (field.getTapType() instanceof TapDate) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                return sdf.format(((DateTime) v).toDate());
            } else if (field.getTapType() instanceof TapDateTime) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                return sdf.format(((DateTime) v).toDate());
            } else if (field.getTapType() instanceof TapYear) {
                return String.valueOf(((DateTime) v).toSqlDate().toLocalDate().getYear());
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            return sdf.format(((DateTime) v).toDate());
        } else if (v instanceof byte[]) {
            byte[] tmp = (byte[]) v;
            if (tmp.length == 0) {
                return null;
            }
            return MD5.create().digestHex(tmp) + "(" + tmp.length + ")";
        }
        return v.toString();
    }
}
