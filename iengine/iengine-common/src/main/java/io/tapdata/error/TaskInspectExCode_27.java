package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 27, module = "Task Inspect", prefix = "TIN", describe = "Task Inspect")
public interface TaskInspectExCode_27 {
	@TapExCode
	String UNKNOWN_ERROR = "27001";

	@TapExCode
	String TABLE_NO_EXISTS = "27002";

	@TapExCode
	String CONNECTOR_NOT_SUPPORT_FUNCTION = "27003";

	@TapExCode
	String PARAM_ERROR = "27004";

	@TapExCode
	String COMMAND_COUNT_PARAM_ERROR = "27005";

	@TapExCode
	String AUTO_RECOVERY_DUPLICATE = "27006";

	@TapExCode
	String AUTO_RECOVERY_NOT_EXISTS = "27007";

    @TapExCode
    String AUTO_RECOVERY_CLIENT_DUPLICATE = "27008";

	@TapExCode
	String AUTO_RECOVERY_TIMEOUT = "27009";

	@TapExCode
	String AUTO_RECOVERY_NOT_FOUND_RECOVERY_TYPE = "27010";

	@TapExCode(
		describe = "Custom query configuration is missing for custom query inspection",
		describeCN = "自定义查询校验缺少查询配置",
		solution = "Configure a valid custom query for both source and target",
		solutionCN = "为源端和目标端配置有效的自定义查询"
	)
	String INSPECT_CUSTOM_QUERY_CONFIG_MISSING = "27011";

	@TapExCode(
		describe = "Query result exceeds the maximum row limit",
		describeCN = "查询结果超过最大行数限制",
		solution = "Reduce the query result size or increase the configured maximum row limit",
		solutionCN = "缩小查询结果范围，或调整允许的最大行数限制"
	)
	String INSPECT_RESULT_EXCEEDS_LIMIT = "27012";
}
