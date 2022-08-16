package io.tapdata.autoinspect.entity;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 21:00 Create
 */
@Setter
@Getter
public class CompareItem implements Serializable {
    private @NonNull String tableName;

    public CompareItem() {
    }

    public CompareItem(@NonNull String tableName) {
        this.tableName = tableName;
    }
}
