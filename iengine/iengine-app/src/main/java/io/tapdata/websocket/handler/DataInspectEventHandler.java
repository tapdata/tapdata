package io.tapdata.websocket.handler;

import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.inspect.Inspect;
import com.tapdata.entity.inspect.InspectStatus;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.inspect.InspectService;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/20 8:34 下午
 * @description
 */
@EventHandlerAnnotation(type = "data_inspect")
public class DataInspectEventHandler extends BaseEventHandler implements WebSocketEventHandler {

	@Override
	public void initialize(ClientMongoOperator clientMongoOperator, SettingService settingService) {
		initialize(clientMongoOperator);
		this.settingService = settingService;
	}

	@Override
	public Object handle(Map event) {

		if (MapUtils.isNotEmpty(event)) {
			Inspect inspect = JSONUtil.map2POJO(event, Inspect.class);

			if (InspectStatus.SCHEDULING.getCode().equalsIgnoreCase(inspect.getStatus())) {
				InspectService.getInstance(clientMongoOperator, settingService).startInspect(inspect);
			} else if (InspectStatus.STOPPING.getCode().equalsIgnoreCase(inspect.getStatus())) {
				InspectService.getInstance(clientMongoOperator, settingService).doInspectStop(inspect.getId());
			}

			return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.EXECUTE_DATA_INSPECT_RESULT, true);
		} else {
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.EXECUTE_DATA_INSPECT_RESULT, "Inspect message can not be empty.");
		}
	}
}
