package com.tapdata.tm.commons.dag.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedList;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TableFieldInfo implements Serializable {
    private String qualifiedName;
    private String originTableName;
    private String previousTableName;
    private Operation operation;
    private LinkedList<FieldInfo> fields;
}
