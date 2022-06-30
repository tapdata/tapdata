package com.tapdata.tm.monitor.vo;

import com.tapdata.tm.monitor.dto.SampleVo;
import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
public class GetMeasurementVo {
    private Map tags;
    private Date date;
    private String grnty;

    private List<SampleVo> samples;
}
