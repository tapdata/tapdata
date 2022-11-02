package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import io.tapdata.entity.schema.TapTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

@Component("dataInspectStrategy")
public class DataInspectStrategyImpl implements DagLogStrategy {
    private static final Logger logger = LogManager.getLogger(DataInspectStrategyImpl.class);
    private static final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.DATA_INSPECT_CHECK;

    @Autowired
    private MetadataInstancesService metadataInstancesService;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        List<TaskDagCheckLog> results = new ArrayList<>();

        // 没开启数据校验不检查
        if (!taskDto.isAutoInspect()) {
            return results;
        }
        if (!taskDto.isCanOpenInspect()) {
            results.add(createWarn(taskDto, userDetail, "任务不支持校验"));
            return results;
        }

        try {
            DAG dag = taskDto.getDag();

            // 不能有处理节点
            List<String> filterNames = new ArrayList<>();
            for (Node<?> node : dag.getNodes()) {
                switch (NodeEnum.valueOf(node.getType())) {
                    case table:
                    case database:
                    case migrate_js_processor:
                    case js_processor:
//                    case table_rename_processor:
//                    case migrate_field_rename_processor:
                        break;
                    default:
                        filterNames.add(node.getName() + "(" + node.getType() + ")");
                        break;
                }
            }
            if (!filterNames.isEmpty()) {
                results.add(createError(taskDto, userDetail, String.format("节点 %s 不支持校验，请删除节点或关闭校验", String.join(",", filterNames))));
            }

            // 数据节点支持查询接口
            // 不能开启动态新增表
            LinkedList<DatabaseNode> sourceNodes = dag.getSourceNode();
            for (DatabaseNode node : sourceNodes) {
                if (null != node.getEnableDynamicTable() && node.getEnableDynamicTable()) {
                    results.add(createError(taskDto, userDetail, String.format("请关闭源 %s 动态新增表，或关闭校验", node.getName())));
                }

//                results.add(createError(taskDto, userDetail, String.format("源 %s 不支持校验", node.getName())));
            }
            for (DatabaseNode node : dag.getTargetNode()) {
                if (null != node.getEnableDynamicTable() && node.getEnableDynamicTable()) {
                    results.add(createError(taskDto, userDetail, String.format("请关闭目标 %s 动态新增表，或关闭校验", node.getName())));
                }
//                results.add(createError(taskDto, userDetail, String.format("目标 %s 不支持校验", node.getName())));
            }

            // 检查通过，统计支持校验表数量
            if (results.isEmpty()) {
                int supportTableCounts = 0;
                int notSupportTableCounts = 0;

                Page<TapTable> tapTables;
                for (DatabaseNode node : sourceNodes) {
                    tapTables = metadataInstancesService.getTapTable(node, userDetail);

                    // 需要分页处理
                    for (TapTable tapTable : tapTables.getItems()) {
                        if (null == tapTable.primaryKeys() || tapTable.primaryKeys().isEmpty()) {
                            notSupportTableCounts++;
                        } else {
                            supportTableCounts++;
                        }
                    }
                }

                results.add(createInfo(taskDto, userDetail, supportTableCounts, notSupportTableCounts));
            }
        } catch (Exception e) {
            logger.warn("校验检查异常：{}", e.getMessage(), e);
            if (e instanceof NullPointerException) {
                StackTraceElement[] stackTraces = e.getStackTrace();
                if (null != stackTraces) {
                    for (StackTraceElement stackTrace : stackTraces) {
                        if (stackTrace.getClassName().contains("tapdata")) {
                            results.add(createError(taskDto, userDetail, "NPE " + stackTrace));
                            return results;
                        }
                    }
                }
            }
            results.add(createError(taskDto, userDetail, "检查异常：" + e.getMessage()));
        }
        return results;
    }

    private static TaskDagCheckLog createInfo(TaskDto taskDto, UserDetail userDetail, int supportTables, int notSupportTables) {
        TaskDagCheckLog checkLog = new TaskDagCheckLog();
        checkLog.setTaskId(taskDto.getId().toHexString());
        checkLog.setCheckType(templateEnum.name());
        checkLog.setCreateAt(new Date());
        checkLog.setCreateUser(userDetail.getUserId());
        checkLog.setGrade(Level.INFO);
        checkLog.setLog(MessageFormat.format(templateEnum.getInfoTemplate(), checkLog.getCreateAt(), taskDto.getName(), supportTables, notSupportTables));
        return checkLog;
    }

    private static TaskDagCheckLog createWarn(TaskDto taskDto, UserDetail userDetail, String msg) {
        TaskDagCheckLog checkLog = new TaskDagCheckLog();
        checkLog.setTaskId(taskDto.getId().toHexString());
        checkLog.setCheckType(templateEnum.name());
        checkLog.setCreateAt(new Date());
        checkLog.setCreateUser(userDetail.getUserId());
        checkLog.setGrade(Level.WARN);
        checkLog.setLog(MessageFormat.format(templateEnum.getErrorTemplate(), checkLog.getCreateAt(), taskDto.getName(), msg));
        return checkLog;
    }

    private static TaskDagCheckLog createError(TaskDto taskDto, UserDetail userDetail, String msg) {
        TaskDagCheckLog checkLog = new TaskDagCheckLog();
        checkLog.setTaskId(taskDto.getId().toHexString());
        checkLog.setCheckType(templateEnum.name());
        checkLog.setCreateAt(new Date());
        checkLog.setCreateUser(userDetail.getUserId());
        checkLog.setGrade(Level.ERROR);
        checkLog.setLog(MessageFormat.format(templateEnum.getErrorTemplate(), checkLog.getCreateAt(), taskDto.getName(), msg));
        return checkLog;
    }

}
