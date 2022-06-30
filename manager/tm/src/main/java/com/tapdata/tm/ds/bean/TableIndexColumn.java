package com.tapdata.tm.ds.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @Author: Zed
 * @Date: 2021/9/29
 * @Description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TableIndexColumn {
    private String columnName;

    private int columnPosition;

    private Boolean columnIsAsc;

    private Object columnValue;
}
