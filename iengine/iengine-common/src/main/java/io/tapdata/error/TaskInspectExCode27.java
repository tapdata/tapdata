package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 27, module = "Task Inspect", prefix = "TIN", describe = "Task Inspect")
public final class TaskInspectExCode27 {
	private TaskInspectExCode27() {
	}

	@TapExCode
	public static final String UNKNOWN_ERROR = "27001";

	@TapExCode
	public static final String TABLE_NO_EXISTS = "27002";

	@TapExCode
	public static final String CONNECTOR_NOT_SUPPORT_FUNCTION = "27003";

	@TapExCode
	public static final String PARAM_ERROR = "27004";

	@TapExCode
	public static final String COMMAND_COUNT_PARAM_ERROR = "27005";

	@TapExCode
	public static final String AUTO_RECOVERY_DUPLICATE = "27006";

	@TapExCode
	public static final String AUTO_RECOVERY_NOT_EXISTS = "27007";

    @TapExCode
    public static final String AUTO_RECOVERY_CLIENT_DUPLICATE = "27008";

	@TapExCode
	public static final String AUTO_RECOVERY_TIMEOUT = "27009";

	@TapExCode
	public static final String AUTO_RECOVERY_NOT_FOUND_RECOVERY_TYPE = "27010";

	@TapExCode(
		describe = "Custom query configuration is missing for custom query inspection",
		describeCN = "自定义查询校验缺少查询配置",
		solution = "Configure a valid custom query for both source and target",
		solutionCN = "为源端和目标端配置有效的自定义查询"
	)
	public static final String INSPECT_CUSTOM_QUERY_CONFIG_MISSING = "27011";

	@TapExCode(
		describe = "Source or target custom query result exceeds the 1000-row limit",
		describeCN = "源端或目标端自定义查询结果超过 1000 行限制",
		solution = "Reduce each source and target query result to no more than 1000 rows",
		solutionCN = "分别缩小源端和目标端查询结果至不超过 1000 行"
	)
	public static final String INSPECT_RESULT_EXCEEDS_LIMIT = "27012";
}
