package com.tapdata.tm.inspect.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.inspect.bean.Stats;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class InspectHistoryListVo extends BaseVo {
    /**  */
    private String status;
    /**  */
    @JsonProperty("inspect_id")
    private String inspect_id;
    private String inspectVersion; // 校验任务版本号，从Inspect继承过来
    /**  */
    private int threads;
    /**  */
    private String agentId;
    /**  */
    private String errorMsg;
    /**  */
    private Double progress;
    /**  */
    @JsonProperty("source_total")
    private Long sourceTotal=0L;
    /**  */
    @JsonProperty("target_total")
    private Long targetTotal=0L;


    private Long firstSourceTotal=0L;       // ": 0, // rr源总数=所有源表数据和
    private Long firstTargetTotal=0L;       // ": 0, // 目标总数=

    /**  */
//    private List<Stats> stats;
    /**  */
    private Long spendMilli;
    /**  */
    private InspectHistoryInspect inspect;
    /**  */
    private Date start;
    /**  */
    private Date end;
    /**  */
    private Date ttlTime;

    @JsonProperty("difference_number")
    private Integer differenceNumber=0;

    /**
     * 实际上egine没有诙谐这个字段值，用的是stats里面的result值
     */
    @Deprecated
    private String result;

    private String firstCheckId; // 初次校验结果编号，表示校验的批次号
    private String parentId; // 父校验结果编号，表示此校验基于 parentId 结果做的二次校验
}

@Data
class InspectHistoryInspect{
    private String inspectMethod;
}
