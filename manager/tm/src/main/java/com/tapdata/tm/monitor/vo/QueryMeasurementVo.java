package com.tapdata.tm.monitor.vo;

import com.tapdata.tm.monitor.dto.SampleVo;
import com.tapdata.tm.monitor.dto.StatisticVo;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class QueryMeasurementVo {

    private List statistics=new ArrayList();
    private List samples=new ArrayList();
}
