package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 11, module = "Task Processor", prefix = "TPR", describe = "Task running processor, include read, write, process node")
public interface TaskProcessorExCode_11 {
	@TapExCode
	String UNKNOWN_ERROR = "11001";

	@TapExCode(
			describe = "Log mining task, failed to write a single log data into the external persistence cache.\n" +
					"Reason\n1. The database configured by the external cache cannot provide normal services",
			solution = "1. On the mining task monitoring interface, find the external cache configuration. Check whether the configuration database is normal",
			describeCN = "日志挖掘任务，将单条日志数据写入外部持久缓存失败。\n" +
					"原因\n1. 外部缓存配置的数据库无法提供正常的服务",
			solutionCN = "1. 在挖掘任务监控界面，找到外部缓存的配置。检查该配置的数据库是否正常"
	)
	String WRITE_ONE_SHARE_LOG_FAILED = "11002";

	@TapExCode
	String CREATE_PROCESSOR_FAILED = "11003";

	@TapExCode(
					describe = "JavaScript node processing error.\n" +
									"Reason\n1. The JavaScript code has an error",
					solution = "1. Review the code and fix errors",
					describeCN = "JavaScript节点处理出错。\n" +
									"原因\n1. JavaScript代码存在错误",
					solutionCN = "1. 检查代码并修正错误"
	)
	String JAVA_SCRIPT_PROCESS_FAILED = "11004";
}
