package io.tapdata.flow.engine.V2.exception;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 36, module = "Concurrent Read Table", prefix = "CRT", describe = "Concurrent Read Table")
public interface TapConcurrentReadTableExCode_36 {
    @TapExCode
    String UNKNOWN_ERROR = "36001";

    @TapExCode(
            describe = "Illegal node type",
            describeCN = "不合法的节点类型"
    )
    String ILLEGAL_NODE_TYPE = "36002";
}
