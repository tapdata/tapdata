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
    //TODO
    @TapExCode(
            describe = "When the dynamic date suffix is enabled on the target node of the development task, the dynamic generation table name rule object cannot be created",
            describeCN = "当开发任务的目标节点开启动态日期后缀时，无法创建动态生成表名规则对象",
            dynamicDescription = "Failed to generate an object for a dynamic table name whose creation rule is {}",
            dynamicDescriptionCN = "生成动态表名规则失败，规则：{}",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String DYNAMIC_INSTANTIATION_FAILED = "35002";

    @TapExCode(
            describe = "Fail to get invocation for dynamic table name",
            describeCN = "当开发任务的目标节点开启动态日期后缀时，创建动态生成表名规则对象时发生报错",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String DYNAMIC_INVOCATION_FAILED = "35003";

    @TapExCode(
            describe = "Dynamic method not find of Dynamic table name generation rule",
            describeCN = "当开发任务目标节点开启动态日期后缀时，找不到动态表名生成规则的生成方法",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String UN_SUPPORT_DYNAMIC_RULE = "35004";

    @TapExCode(
            describe = "Can not access dynamic table name generation method",
            describeCN = "当开发任务目标节点开启动态日期后缀时，无法调用动态表名生成规则的生成方法时",
            solution = "Please select a supported dynamic table name generation rule",
            solutionCN = "请选择一个已支持的动态表名生成规则",
            type = TapExType.PARAMETER_INVALID,
            level = TapExLevel.NORMAL
    )
    String DYNAMIC_METHOD_ACCESS_FAILED = "35005";

    @TapExCode(
            describe = "The conflict between the table renaming DDL event and the table renaming node in the task is identified in the source node",
            describeCN = "源节点中识别出表改名DDL事件与任务中的表编辑节点产生冲突",
            solution = "1. Modify the task configuration to remove the table editor node\n+" +
                    "2. Change the -Advanced Settings -DDL synchronization configuration in the node configuration of the source node to automatically ignore all DDLS, so that the source node will ignore DDL events and will not conflict with the table editor node",
            solutionCN = "1. 修改任务配置，删除表编辑节点" +
                    "2. 将源节点的节点配置中-高级设置-DDL同步配置修改为自动忽略所有DDL，这样源节点将忽略DDL事件，不会与表编辑节点产生冲突",
            dynamicDescription = "Cannot apply the table renaming DDL because it conflicts with the custom table name in the table editor node, from {} to {}",
            dynamicDescriptionCN = "不能应用表重命名DDL，因为它与表编辑节点中的自定义表名冲突，从 {} 到 {}"
    )
    String RENAME_DDL_CONFLICTS_WITH_CUSTOM_TABLE_NAME = "35006";
}
