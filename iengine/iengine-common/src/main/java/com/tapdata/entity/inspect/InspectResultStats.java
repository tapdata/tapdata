package com.tapdata.entity.inspect;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/20 3:03 下午
 * @description
 */
@Setter
@Getter
@ToString
@EqualsAndHashCode
public class InspectResultStats implements Serializable {

	private String taskId;
	private InspectDataSource source;
	private InspectDataSource target;
	private Date start;
	private Date end;
	private String status;
	private String errorMsg;
	private String result;
	private double progress;
	private long cycles;
	private long firstSourceTotal;
	private long firstTargetTotal;
	private long source_total;
	private long target_total;
	private long both;
	private long source_only;
	private long target_only;
	private long row_passed;
	private long row_failed;
	private long speed;
	// 增量校验时使用，增量运行配置
	private InspectCdcRunProfiles cdcRunProfiles;

}
