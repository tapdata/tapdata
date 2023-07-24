package io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-07-20 14:39
 **/
@TapExClass(code = 25, module = "Dynamic Adjust Memory", describe = "The module calculates the appropriate size of the memory queue by calculating the memory usage size of the data row", prefix = "DAM")
public interface DynamicAdjustMemoryExCode_25 {
	@TapExCode
	String UNKNOWN_ERROR = "25001";
}
