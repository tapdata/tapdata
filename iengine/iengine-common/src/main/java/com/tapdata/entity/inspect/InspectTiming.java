package com.tapdata.entity.inspect;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/9 12:56 上午
 * @description
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class InspectTiming {

	private int intervals;  // 间隔时间
	private String intervalsUnit;  // second、minute、hour、day、week、month
	private String start;  // 开始时间，设置以后每次触发的执行时间
	private String end;

}
