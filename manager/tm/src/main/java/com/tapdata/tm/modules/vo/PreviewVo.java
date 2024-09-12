package com.tapdata.tm.modules.vo;

import lombok.Data;

@Data
public class PreviewVo {
    private Long totalCount = 0L;
    //总共访问次数
    private Long visitTotalCount = 0L;

    //告警的访问次数
    private Long warningVisitTotalCount = 0L;

    private Long visitTotalLine = 0L;
    private Long transmitTotal = 0L;
    private Long warningApiCount = 0L;
    private Long lastUpdAt = 0L;
}
