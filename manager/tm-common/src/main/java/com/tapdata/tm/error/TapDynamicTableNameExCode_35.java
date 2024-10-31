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
            describe = "This error occurs when a dynamic date suffix cannot be created to generate a dynamic table name object when the target table of the development task is enabled",
            describeCN = "当开发任务的目标节点开启动态日期后缀时，当无法创建动态生成表名规则对象时，会出现此错误",
            dynamicDescription = "Failed to generate an object for a dynamic table name whose creation rule is {}",
            dynamicDescriptionCN = "创建规则为{} 的动态表名生成对象时失败",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String DYNAMIC_INSTANTIATION_FAILED = "35002";

    @TapExCode(
            describe = "Fail to get invocation for dynamic table name",
            describeCN = "当开发任务的目标节点开启动态日期后缀时，当创建动态生成表名规则对象时发生报错，则会抛出此错误",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String DYNAMIC_INVOCATION_FAILED = "35003";

    @TapExCode(
            describe = "Dynamic method not find of Dynamic table name generation rule",
            describeCN = "当开发任务目标节点开启动态日期后缀时，找不到动态表名生成规则的生成方法时，则会报此错误",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String UN_SUPPORT_DYNAMIC_RULE = "35004";

    @TapExCode(
            describe = "Can not access dynamic table name generation method",
            describeCN = "当开发任务目标节点开启动态日期后缀时，无法调用动态表名生成规则的生成方法时，则会报此错误",
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
            describeCN = "当复制任务有表编辑节点时，并且源端出现表改名DDL操作时，表编辑节点与改名DDL会发生冲突，则会报此错误",
            solutionCN = "1、修改任务配置，删除表编辑节点" +
                    "2、修改任务配置，在源节点-高级设置中-忽略表改名DDL事件",
            dynamicDescription = "Can't apply table rename DDL because it conflicts with custom-table-name from {} to {}",
            dynamicDescriptionCN = "不能应用表重命名DDL，因为它与表编辑自定义表名冲突，从 {} 到 {}"
    )
    String RENAME_DDL_CONFLICTS_WITH_CUSTOM_TABLE_NAME = "35006";
}
