package com.tapdata.tm.inspect.param;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.inspect.bean.Stats;
import com.tapdata.tm.inspect.dto.InspectDto;
import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
public class SaveInspectResultParam {
    private String id;
    private String inspectVersion;
    public static String STATUS_ERROR = "error";
    public static String STATUS_DONE = "done";


    public static String RESULT_PASSED = "passed";
    public static String RESULT_FAILED = "failed";
    /**  */
    private String status;
    /**  */
    @JsonProperty("inspect_id")
    private String inspect_id;
    /**  */
    private String threads;
    /**  */
    private String agentId;
    /**  */
    private String errorMsg;
    /**  */
    private Double progress;
    /**  */
    private Long source_total=0L;
    /**  */
    private Long target_total=0L;


    private Long firstSourceTotal=0L;       // ": 0, // rr源总数=所有源表数据和
    private Long firstTargetTotal=0L;       // ": 0, // 目标总数=

    /**  */
    private List<Map> stats;
    /**  */
    private Integer spendMilli;
    /**  */
    private Map inspect;
    /**  */
    private Long start;
    /**  */
    private Long end;
    /**  */
    private Date ttlTime;

    @JsonProperty("difference_number")
    private Integer differenceNumber=0;

    /**
     * 实际上egine没有诙谐这个字段值，用的是stats里面的result值
     */
    @Deprecated
    private String result;
    private  String user_id;
    private String customId;
    private  String parentId;
    private String firstCheckId;
}
