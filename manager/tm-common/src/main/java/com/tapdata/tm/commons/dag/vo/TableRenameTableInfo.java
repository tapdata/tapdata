package com.tapdata.tm.commons.dag.vo;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TableRenameTableInfo implements Serializable {
    private String originTableName;
    private String previousTableName;
    private String currentTableName;
}


