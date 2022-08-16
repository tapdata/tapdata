package io.tapdata.autoinspect.entity;

import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import io.tapdata.autoinspect.constants.CompareResultEnums;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.bson.types.ObjectId;

import java.util.*;

@Setter
@Getter
public class CompareRecord {

    private @NonNull LinkedHashMap<String, Object> originalKey;
    private @NonNull LinkedHashSet<String> keyNames;
    private @NonNull Map<String, Object> data;

    public CompareRecord() {
        this.originalKey = new LinkedHashMap<>();
        this.keyNames = new LinkedHashSet<>();
        this.data = new HashMap<>();
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
    public CompareResultEnums compareKeys(@NonNull CompareRecord o) {
        int compareValue;
        Object v1, v2;
        for (String k : getKeyNames()) {
            v1 = getDataValue(k);
            v2 = o.getDataValue(k);
            if (null == v1) {
                if (null == v2) continue;
                return CompareResultEnums.Less;
            } else if (null == v2) {
                return CompareResultEnums.Lager;
            }

            compareValue = v1.hashCode() - v2.hashCode();
            if (0 < compareValue) {
                return CompareResultEnums.Lager;
            } else if (0 > compareValue) {
                return CompareResultEnums.Less;
            }
        }

        return CompareResultEnums.Same;
    }

    /**
     * compare data and filter keys, call after compareKeys
     *
     * @param taskId    task id
     * @param tableName data table name
     * @param o         target value
     * @return return compare result if difference
     */
    public TaskAutoInspectResultDto compareData(@NonNull String taskId, @NonNull ObjectId sourceConnId, @NonNull ObjectId targetConnId, @NonNull String tableName, @NonNull CompareRecord o) {
        Object odata;
        List<String> diffKeys = new ArrayList<>();
        Map<String, Object> omap = o.getData();
        for (Map.Entry<String, Object> en : data.entrySet()) {
            //filter keys
            if (keyNames.contains(en.getKey())) continue;

            //check null value
            odata = omap.get(en.getKey());
            if (null == en.getValue()) {
                if (null != odata) {
                    diffKeys.add(en.getKey());
                }
                continue;
            }

            //check value
            if (!en.getValue().equals(odata)) {
                diffKeys.add(en.getKey());
            }
        }

        //has not difference return null
        if (diffKeys.isEmpty()) {
            return null;
        }
        return new TaskAutoInspectResultDto(taskId, sourceConnId, targetConnId, tableName, getOriginalKey(), getData(), o.getData());
    }

}
