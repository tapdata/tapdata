package io.tapdata.flow.engine.V2.cleaner;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.service.skeleton.annotation.RemoteService;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-01-03 12:20
 **/
@RemoteService
public class CleanService {
	public Map<String, Object> cleanTaskNode(String taskId, String nodeId, String type) {
		CleanTypeEnum cleanTypeEnum;
		try {
			cleanTypeEnum = CleanTypeEnum.valueOf(type);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Nonsupport cleaner type: " + type, e);
		}
		Class<? extends ICleaner> cleanerClz = cleanTypeEnum.getCleanerClz();
		ICleaner iCleaner;
		try {
			iCleaner = cleanerClz.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, String.format("Instant %s failed", cleanerClz.getName()), e);
		}
		CleanResult cleanResult = iCleaner.cleanTaskNode(taskId, nodeId);
		try {
			return MapUtil.obj2Map(cleanResult);
		} catch (IllegalAccessException e) {
			Map<String, Object> failedResultMap = new HashMap<>();
			failedResultMap.put("result", 2);
			failedResultMap.put("errorMessage", String.format("Failed convert clean result to map, clean result: %s", cleanResult));
			failedResultMap.put("errorStack", Log4jUtil.getStackString(e));
			return failedResultMap;
		}
	}
}
