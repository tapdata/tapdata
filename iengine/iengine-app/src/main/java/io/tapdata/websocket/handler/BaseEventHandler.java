package io.tapdata.websocket.handler;

import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.websocket.WebSocketEventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author samuel
 * @Description
 * @create 2020-09-23 17:29
 **/
public abstract class BaseEventHandler implements WebSocketEventHandler {
	protected static Logger logger = LogManager.getLogger(BaseEventHandler.class);
	protected static final String EVENT_DATA = "data";
	protected ClientMongoOperator clientMongoOperator;
	protected SettingService settingService;

	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void initialize(ClientMongoOperator clientMongoOperator, SettingService settingService) {
		this.clientMongoOperator = clientMongoOperator;
		this.settingService = settingService;
	}
}
