package com.tapdata.tm.modules.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class ApiDetailVo {
    private Long visitTotalCount = 0L;
    private Long errorVisitTotalCount = 0L;
    private Long visitQuantity = 0L;
    private Long timeConsuming = 0L;
    private Long visitTotalLine = 0L;
//    api  最新一次调用耗费了多少时间   /  传输了多少条数据
    private Double responseTime = 0D;
    private Double speed = 0D;

    private List<Long> time = new ArrayList<>();
    private List<Object> value = new ArrayList<>();

    private List<Map<String,String>> clientNameList = new ArrayList<>();
}
