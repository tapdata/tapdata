package io.tapdata.websocket.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 32, module = "WebSocket Handler", prefix = "wsh", describe = "")
public interface WebSocketHandlerExCode_32 {
    @TapExCode
    String PATH_TO_URL_FAILED = "32001";
}
