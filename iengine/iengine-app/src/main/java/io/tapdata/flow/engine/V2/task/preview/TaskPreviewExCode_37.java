package io.tapdata.flow.engine.V2.task.preview;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2024-09-27 12:10
 **/
@TapExClass(code = 37, module = "Task Preview", describe = "Task Preview Exception", prefix = "TP")
public interface TaskPreviewExCode_37 {
	@TapExCode
	String UNKNOWN = "37001";
	@TapExCode
	String MERGE_QUERY_ADVANCE_FILTER_ERROR = "37002";
	@TapExCode
	String MERGE_BATCH_READ_ERROR = "37003";
	@TapExCode
	String NOT_FOUND_SOURCE_TAP_TABLE = "37004";
	@TapExCode
	String NODE_TYPE_INVALID = "37005";
	@TapExCode
	String MEMORY_MERGE_ERROR = "37006";
}
