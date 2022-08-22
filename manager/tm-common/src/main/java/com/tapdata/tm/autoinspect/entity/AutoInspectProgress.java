package com.tapdata.tm.autoinspect.entity;

import com.tapdata.tm.autoinspect.constants.CompareStep;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;
import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/17 10:51 Create
 */
@Data
public class AutoInspectProgress implements Serializable {

    private int tableCounts;
    private @NonNull CompareStep status;
    private @NonNull List<CompareTableItem> tableItems;

    public AutoInspectProgress(int tableCounts, @NonNull List<CompareTableItem> tableItems) {
        this.status = CompareStep.Initial;
        this.tableCounts = tableCounts;
        this.tableItems = tableItems;
    }
}
