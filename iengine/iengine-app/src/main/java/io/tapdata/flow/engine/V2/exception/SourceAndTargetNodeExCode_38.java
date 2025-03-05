package io.tapdata.flow.engine.V2.exception;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2024-11-20 15:57
 **/
@TapExClass(code = 38, module = "Source and Target Node", describe = "Source and Target Node", prefix = "STN")
public interface SourceAndTargetNodeExCode_38 {
	@TapExCode
	String UNKNOWN_ERROR = "38001";

	@TapExCode(
			describe = "Failed to start reading source data node",
			describeCN = "启动读取源数据节点失败",
			dynamicDescription = "node name: {}, connection name: {}",
			dynamicDescriptionCN = "节点名称: {}, 连接名称: {}"
	)
	String INIT_SOURCE_ERROR = "38002";

	@TapExCode(
			describe = "Failed to start writing to data node",
			describeCN = "启动写入数据节点失败",
			dynamicDescription = "node name: {}, connection name: {}",
			dynamicDescriptionCN = "节点名称: {}, 连接名称: {}"
	)
	String INIT_TARGET_ERROR = "38003";
}
