package io.tapdata.websocket.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 32, module = "WebSocket Handler", prefix = "WSH", describe = "")
public interface WebSocketHandlerExCode_32 {
    @TapExCode(
            describe = "Failed to convert path to URL",
            describeCN = "无法将路径转换为URL"
    )
    String PATH_TO_URL_FAILED = "32001";
}
