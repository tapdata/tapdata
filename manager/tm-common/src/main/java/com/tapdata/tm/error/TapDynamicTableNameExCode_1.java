package com.tapdata.tm.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import io.tapdata.exception.TapExLevel;
import io.tapdata.exception.TapExType;

/**
 * @author gavin
 * @create 2023-03-26 15:20
 **/
@TapExClass(code = 35, module = "Dynamic Table Name", prefix = "DTN", describe = "Dynamic Table Name")
public interface TapDynamicTableNameExCode_1 {
    @TapExCode
    String UNKNOWN_ERROR = "35001";

    @TapExCode(
            describe = "Dynamic table name generation rule not supported",
            describeCN = "动态表名生成规则不支持",
            solution = "Please select a supported dynamic table name generation rule",
            solutionCN = "请选择一个已支持的动态表名生成规则",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String UN_SUPPORT_DYNAMIC_RULE = "35002";
}
