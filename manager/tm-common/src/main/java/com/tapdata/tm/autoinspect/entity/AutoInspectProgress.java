package com.tapdata.tm.autoinspect.entity;

import com.tapdata.tm.autoinspect.constants.CompareStep;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/17 10:51 Create
 */
@Data
public class AutoInspectProgress implements Serializable {

    private int tableCounts;//任务总表数量
    private int tableIgnore;//不支持校验表数量
    private @NonNull CompareStep step;
    private @NonNull Map<String, CompareTableItem> tableItems;//表校验状态

    public AutoInspectProgress() {
        this.step = CompareStep.Initial;
        this.tableItems = new LinkedHashMap<>();
    }

    public void addTableCounts(int size) {
        this.tableCounts += size;
    }

    public void addTableIgnore(int size) {
        this.tableIgnore += size;
    }

    public void addTableItem(@NonNull CompareTableItem item) {
        this.tableItems.put(item.getTableName(), item);
    }

    public CompareTableItem getTableItem(@NonNull String tableName) {
        return this.tableItems.get(tableName);
    }
}
