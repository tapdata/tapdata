package io.tapdata.flow.engine.V2.exception;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 36, module = "Concurrent Read Table", prefix = "CRT", describe = "Concurrent Read Table")
public interface TapConcurrentReadTableExCode_36 {
    @TapExCode
    String UNKNOWN_ERROR = "36001";

    @TapExCode(
            describe = "The illegal node type is checked when multiple concurrent reads are enabled on the source side",
            describeCN = "初始化多并发源节点失败，非预期的节点类型",
            dynamicDescription = "Expected node type {}, actual node type {}",
            dynamicDescriptionCN = "预期节点类型为{} ，实际节点类型为{}"
    )
    String ILLEGAL_NODE_TYPE = "36002";
}
