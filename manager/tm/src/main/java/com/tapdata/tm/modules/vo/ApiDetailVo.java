package com.tapdata.tm.modules.vo;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ApiDetailVo {
    private Long visitTotalCount;
    private Long errorVisitTotalCount;
    private Long visitQuantity;
    private Long timeConsuming;
    private Long visitTotalLine;
//    api  最新一次调用耗费了多少时间   /  传输了多少条数据
    private Long responseTime;
    private Long speed;

    private List<Long> time;
    private List<Object> value;

    private List<Map<String,String>> clientNameList;
}
