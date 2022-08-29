package com.tapdata.tm.autoinspect.entity;

import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 21:00 Create
 */
@Data
public class CompareTableItem implements Serializable {
    private @NonNull String tableName;//表名
    private boolean initialed;//是否完成全量校验
    private @NonNull Set<LinkedHashMap<String, Object>> diffKeys;
    private Object offset;

    public CompareTableItem() {
        this.initialed = false;
        this.diffKeys = new LinkedHashSet<>();
    }

    public CompareTableItem(@NonNull String tableName) {
        this();
        this.tableName = tableName;
    }

    public int getDiffCounts() {
        return this.diffKeys.size();
    }

    public void addDiff(LinkedHashMap<String, Object> keymap) {
        this.diffKeys.add(keymap);
    }

    public void removeDiff(LinkedHashMap<String, Object> keymap) {
        this.diffKeys.remove(keymap);
    }
}
