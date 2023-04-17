package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 19, module = "Target Share CDC Processor", prefix = "TSCP", describe = "Write share cdc log content")
public interface TaskTargetShareCDCProcessorExCode_19 {
	@TapExCode
	String UNKNOWN_ERROR = "19001";
	@TapExCode(
			describe = "Failed to insert multiple records",
			describeCN = "批量插入记录失败"
	)
	String INSERT_MANY_INTO_RINGBUFFER_FAILED = "19002";
	@TapExCode(
			describe = "Failed to convert log content to document",
			describeCN = "将日志内容转换为文档失败"
	)
	String CONVERT_LOG_CONTENT_TO_DOCUMENT_FAILED = "19003";
}
