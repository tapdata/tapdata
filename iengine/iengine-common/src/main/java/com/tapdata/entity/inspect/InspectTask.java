package com.tapdata.entity.inspect;

import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/7 10:30 上午
 * @description
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class InspectTask implements Serializable {

	private String taskId; // "1",  // 子任务id，可以objectid/uuid/数字，任务内唯一
	private boolean fullMatch = false; // false,  // 是否执行行匹配

	/**
	 * 自定义行数据比对脚本， null - 使用默认实现
	 * 自定义样例：function(sourceRecord, targetRecord){ return true; }
	 */
	private String compareFn;

	/**
	 * 样例：function(inspect_result){ return true; }
	 */
	private String confirmFn; // null,    // 用户可以自定义通过条件

	private int batchSize = 10000;

	private InspectDataSource source;
	private InspectDataSource target;

	private InspectLimit limit;

	// 增量校验时使用，增量运行配置
	private InspectCdcRunProfiles cdcRunProfiles;

	/**
	 * 自定义校验脚本
	 * 自定义样例: function validate(sourceRow) { return {result: result, message: message, data: targetRow}; }
	 */
	private String script;

	/**
	 * 是否开启高级校验
	 */
	private boolean showAdvancedVerification;

	/**
	 * When verifying all fields, specify the data types that do not participate in the comparison
	 */
	private Boolean enableIgnoreType = CommonUtils.getPropertyBool("INSPECT_ENABLE_IGNORE_TYPE", true);
	private List<String> ignoredType;
}
