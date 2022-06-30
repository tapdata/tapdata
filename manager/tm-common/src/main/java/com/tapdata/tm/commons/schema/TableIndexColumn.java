package com.tapdata.tm.commons.schema;

import lombok.Data;

import java.io.Serializable;

/**
 * @Author: Zed
 * @Date: 2021/9/29
 * @Description:
 */
@Data
public class TableIndexColumn implements Serializable {
    private String columnName;

    private int columnPosition;

    private Boolean columnIsAsc;

    private Object columnValue;
}
