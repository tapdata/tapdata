package com.tapdata.tm.commons.dag.vo;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class TableRenameTableInfo implements Serializable {
    private String originTableName;
    private String previousTableName;
    private String currentTableName;

    public TableRenameTableInfo() {
    }

    public TableRenameTableInfo(String originTableName, String previousTableName, String currentTableName) {
        this.originTableName = originTableName;
        this.previousTableName = previousTableName;
        this.currentTableName = currentTableName;
    }
}


