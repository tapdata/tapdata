package com.tapdata.entity.inspect;

import com.tapdata.entity.BaseEntity;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/20 2:59 下午
 * @description
 */
@Setter
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class InspectResult extends BaseEntity implements Serializable {

	private String inspect_id;
	private String inspectVersion; // 校验任务版本号，从Inspect继承过来
	private int threads;
	private String status;    // 有一个在运行，就是running；所有运行完，才为done。
	private String agentId;    // 执行本次校验的 agentId

	private String errorMsg;
	private double progress;
	private long firstSourceTotal;
	private long firstTargetTotal;
	private long source_total;
	private long target_total;
	private List<InspectResultStats> stats;
	private long spendMilli;
	private Inspect inspect;
	private Date start;
	private Date end;
	private String user_id;
	private String customId;

	private String firstCheckId; // 初次校验结果编号，表示校验的批次号
	private String parentId; // 父校验结果编号，表示此校验基于 parentId 结果做的二次校验

	private boolean partStats;  // 标识是否只上报了一部分 stats 信息
}
