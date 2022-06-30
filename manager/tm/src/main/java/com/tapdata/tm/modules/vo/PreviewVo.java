package com.tapdata.tm.modules.vo;

import lombok.Data;

@Data
public class PreviewVo {
    private Long totalCount;
    //总共访问次数
    private Long visitTotalCount;

    //告警的访问次数
    private Long warningVisitTotalCount;

    private Long visitTotalLine;
    private Long transmitTotal;
    private Long warningApiCount;

}
