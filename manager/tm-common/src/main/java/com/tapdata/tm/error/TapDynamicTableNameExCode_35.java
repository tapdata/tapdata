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
public interface TapDynamicTableNameExCode_35 {
    @TapExCode
    String UNKNOWN_ERROR = "35001";

    @TapExCode(
            describe = "Fail to get instantiation for dynamic table name",
            describeCN = "无法获取动态表名的实例化对象",
            solution = "Please select a supported dynamic table name generation rule",
            solutionCN = "请选择一个已支持的动态表名生成规则",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String DYNAMIC_INSTANTIATION_FAILED = "35002";

    @TapExCode(
            describe = "Fail to get invocation for dynamic table name",
            describeCN = "无法获取对动态表名称的调用方法",
            solution = "Please select a supported dynamic table name generation rule",
            solutionCN = "请选择一个已支持的动态表名生成规则",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String DYNAMIC_INVOCATION_FAILED = "35003";

    @TapExCode(
            describe = "Dynamic method not find of Dynamic table name generation rule",
            describeCN = "找不到动态表名生成规则的动态方法",
            solution = "Please select a supported dynamic table name generation rule",
            solutionCN = "请选择一个已支持的动态表名生成规则",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String UN_SUPPORT_DYNAMIC_RULE = "35004";

    @TapExCode(
            describe = "Can not access dynamic table name generation method",
            describeCN = "无法访问动态表名生成方法",
            solution = "Please select a supported dynamic table name generation rule",
            solutionCN = "请选择一个已支持的动态表名生成规则",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String DYNAMIC_METHOD_ACCESS_FAILED = "35005";

    @TapExCode(
        describe = "Table renamed DDL conflicts with the custom table name.\n" +
            "Reason:\n1. The task assigns a new table name to the table, which cannot be automatically processed when the table is renamed DDL.",
        solution = "1. The task configuration needs to be analyzed and resolved.",
        describeCN = "表改名DDL与自定义表名冲突\n" +
            "原因\n1. 任务为表指定新表名，这个表发生改名DDL时无法自动处理",
        solutionCN = "1. 需要分析任务配置，并进行解决"
    )
    String RENAME_DDL_CONFLICTS_WITH_CUSTOM_TABLE_NAME = "35006";
}
