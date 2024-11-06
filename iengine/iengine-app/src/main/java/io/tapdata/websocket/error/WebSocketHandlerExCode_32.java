package io.tapdata.websocket.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 32, module = "WebSocket Handler", prefix = "WSH", describe = "")
public interface WebSocketHandlerExCode_32 {
    @TapExCode(
            describe = "When a jar is loaded dynamically, there is no way to convert the jar path to a URL",
            describeCN = "在动态加载jar包时，无法将jar包的路径转换为URL",
            solution = "The --workDir argument specifies a valid working directory when the system starts",
            solutionCN = "在系统启动时，通过--workDir 参数指定合法的工作目录"
    )
    String PATH_TO_URL_FAILED = "32001";
}
