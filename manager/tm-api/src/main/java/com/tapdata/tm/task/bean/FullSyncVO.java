package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.Date;


/**
 * @Author: Zed
 * @Date: 2021/12/2
 * @Description: 全量同步返回实体
 */
@Data
public class FullSyncVO {
    /** 总表数 */
    private Integer totalTaleNum;
    /** 总行数 */
    private Long totalDataNum;
    /** 成功的表数 */
    private Integer completeTaleNum;
    /** 成功的行数 */
    private Long finishNumber;
    /** 开始时间 */
    private Date startTs;
    /** 结束时间*/
    private Date endTs;
    /** 当前时间  */
    private Date currentTime;
    /** 预计完成所需要的时间 单位秒 */
    private Long finishDuration = 0L;
    private String srcNodeId;
    private String tgtNodeId;
    private String srcConnId;
    private String tgtConnId;
    /** 当前进度 */
    private Double progress;
}
