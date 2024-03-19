package io.tapdata.websocket;


import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.task.TaskService;

import java.util.Map;


/**
 * 事件处理接口
 *
 * @author jackin
 */
public interface WebSocketEventHandler<T> {

	/**
	 * 初始化handler方法
	 *
	 * @param clientMongoOperator 查询管理端数据
	 */
	void initialize(ClientMongoOperator clientMongoOperator);


	default void initialize(TaskService<TaskDto> taskService, ClientMongoOperator clientMongoOperator, SettingService settingService) {
		initialize(clientMongoOperator, settingService);
	}

	/**
	 * 初始化handler方法
	 *
	 * @param clientMongoOperator 查询管理端数据
	 * @param settingService      系统配置常量
	 */
	default void initialize(ClientMongoOperator clientMongoOperator, SettingService settingService) {
		initialize(clientMongoOperator);
	}

	/**
	 * 处理事件方法
	 *
	 * @param event 事件
	 * @return 返回处理结果
	 */
	default T handle(Map event) {
		return null;
	}

	/**
	 * 处理事件方法，可以在处理中多次返回消息体
	 * 优先调用该方法，如果没有实现，则调用{@link WebSocketEventHandler#handle(java.util.Map)}
	 *
	 * @param event
	 * @param sendMessage
	 * @return
	 */
	default T handle(Map event, SendMessage<WebSocketEventResult> sendMessage){
		return handle(event);
	}

}
