package com.tapdata.tm.taskinspect.vo;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
public class CheckItemInfo implements Serializable {

    private String rowId;
    private String tableName;
    private String keys;

}
