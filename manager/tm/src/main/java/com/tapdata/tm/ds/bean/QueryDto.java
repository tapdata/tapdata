package com.tapdata.tm.ds.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description:
 */
@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class QueryDto {
    /** 当前页 */
    protected int pageNum = 1;
    /** 每页多少条 */
    protected int pageSize = 20;
}
