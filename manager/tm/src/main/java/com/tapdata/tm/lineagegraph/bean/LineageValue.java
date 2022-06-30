package com.tapdata.tm.lineagegraph.bean;

import lombok.Data;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/10/20
 * @Description:
 */
@Data
public class LineageValue {
    private String type;
    private List<DataFlow> dataFlows;
}
