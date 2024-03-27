
package com.tapdata.tm.inspect.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.inspect.bean.Stats;
import com.tapdata.tm.inspect.dto.InspectDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;


/**
 * 数据校验结果
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("InspectResult")
public class InspectResultEntity extends BaseEntity {
    private String inspect_id;      // 关联键
    private int threads;            // 并行线程数
    private String status;          // "running/done/error",  // 有一个在运行，就是running；所有运行完，才为done。
    private String errorMsg;        // 状态为 error 时有效
    private Double progress;         // ": 0.8,  // 进度，完成了百分之八十，所有表的 progress 的平均值

    //long的话会查询不到，原因待验证
    private Long sourceTotal;       // ": 0, // rr源总数=所有源表数据和
    private Long targetTotal;       // ": 0, // 目标总数=


    private Long firstSourceTotal;       // ": 0, // rr源总数=所有源表数据和
    private Long firstTargetTotal;       // ": 0, // 目标总数=


    private String agentId;         //": "",     // 执行本次校验的 agent id
    private List<Stats> stats;         // 统计指标
    // 主键相等记录数、源库有目标库无的记录数，目标库有源库无的记录数，源库总记录数，目标库总记录数，循环比对次数，行数据校验相等记录数，行数据校验不相等记录数

    private long spendMilli;
    private Long start;
    private Long end;
    private Date ttlTime;
    private String firstCheckId; // 初次校验结果编号，表示校验的批次号
    private String parentId; // 父校验结果编号，表示此校验基于 parentId 结果做的二次校验

    private InspectDto inspect;

}
