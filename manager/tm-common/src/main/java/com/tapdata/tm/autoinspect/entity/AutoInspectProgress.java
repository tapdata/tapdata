package com.tapdata.tm.autoinspect.entity;

import com.tapdata.tm.autoinspect.constants.CompareStep;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/17 10:51 Create
 */
@Data
public class AutoInspectProgress implements Serializable {

    private int tableCounts;//任务总表数量
    private int tableIgnore;//不支持校验表数量
    private @NonNull CompareStep step;
    private @NonNull List<CompareTableItem> tableItems;//表校验状态

    public AutoInspectProgress() {
        this.step = CompareStep.Initial;
        this.tableItems = new ArrayList<>();
    }

    public void addTableCounts(int size) {
        this.tableCounts += size;
    }

    public void addTableIgnore(int size) {
        this.tableIgnore += size;
    }

    public void addTableItem(CompareTableItem item) {
        this.tableItems.add(item);
    }
}
