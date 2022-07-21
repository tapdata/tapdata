package io.tapdata.websocket.handler;

import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.task.DataQaulityStatsTask;
import io.tapdata.task.TaskContext;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-04-21 22:31
 **/
@EventHandlerAnnotation(type = "analyzeQuality")
public class AnalyzeQualityHandler extends DataQaulityStatsTask implements WebSocketEventHandler {

	private static Logger logger = LogManager.getLogger(AnalyzeQualityHandler.class);

	/**
	 * 初始化handler方法
	 *
	 * @param clientMongoOperator 查询管理端数据
	 */
	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {

	}

	/**
	 * 初始化handler方法
	 *
	 * @param clientMongoOperator 查询管理端数据
	 * @param settingService      系统配置常量
	 */
	@Override
	public void initialize(ClientMongoOperator clientMongoOperator, SettingService settingService) {
		taskContext = new TaskContext(clientMongoOperator, settingService);
	}

	/**
	 * 处理事件方法
	 *
	 * @param event 事件
	 * @return 返回处理结果
	 */
	@Override
	public Object handle(Map event) {
		List<String> connectionIds = null;
		try {
			connectionIds = (List<String>) event.get("connectionIds");
		} catch (Exception e) {
			logger.error("Analyze data quality by connection id failed, input parm is invalid: {}", event);
		}
		if (CollectionUtils.isNotEmpty(connectionIds)) {
			super.initialize(taskContext);
			super.connectionIds = connectionIds;
			super.qualityAnalysis();
		}

		return null;
	}
}
