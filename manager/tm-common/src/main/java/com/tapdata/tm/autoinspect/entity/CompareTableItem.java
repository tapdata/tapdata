package com.tapdata.tm.autoinspect.entity;

import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 21:00 Create
 */
@Data
public class CompareTableItem implements Serializable {
    private @NonNull String tableName;
    private boolean initialed;
    private Object offset;

    public CompareTableItem() {
        this.initialed = false;
    }

    public CompareTableItem(@NonNull String tableName) {
        this();
        this.tableName = tableName;
    }
}
