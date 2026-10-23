package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.ConnectionAgentConflictReport;
import com.tapdata.tm.ds.exception.ConnectionAgentConflictException;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MessageUtil;
import lombok.Setter;
import io.github.openlg.graphlib.Graph;
import com.tapdata.tm.commons.dag.Edge;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

/** Reads proposed settings without persisting connections or changing task policies. */
@Service
@Setter
public class ConnectionAgentConflictService {
    private static final int SCAN_SIZE = 200;
    @Autowired @Lazy private TaskService taskService;
    @Autowired @Lazy private TaskScheduleService taskScheduleService;
    @Autowired @Lazy private InspectService inspectService;

    public void assertNoConflicts(Map<String, DataSourceConnectionDto> overrides, UserDetail user) {
        ConnectionAgentConflictReport report = findConflicts(overrides, user, 0, 20);
        if (report.isHasConflicts()) throw new ConnectionAgentConflictException(report);
    }

    public ConnectionAgentConflictReport findConflicts(Map<String, DataSourceConnectionDto> overrides,
                                                       UserDetail user, int skip, int limit) {
        if (skip < 0 || limit < 1 || limit > 100) throw new BizException("IllegalArgument", "Invalid conflict pagination");
        ConnectionAgentConflictReport report = new ConnectionAgentConflictReport();
        if (overrides.isEmpty()) return report;
        ObjectId after = null;
        while (true) {
            Query query = candidates(user, after, new Criteria().orOperator(
                    Criteria.where("dag.nodes.connectionId").in(overrides.keySet()),
                    Criteria.where("dag.nodes.connectionIds").in(overrides.keySet())));
            query.fields().include("_id", "name", "syncType", "status", "dag", "accessNodeType",
                    "accessNodeProcessId", "priorityProcessId", "user_id", "shareCache");
            List<TaskDto> tasks = taskService.findAll(query);
            if (tasks == null || tasks.isEmpty()) break;
            for (TaskDto task : tasks) {
                if (task.usesConnectionAgentPolicy()) collect(report, task, task.getSyncType(), overrides, user, skip, limit);
            }
            if (tasks.size() < SCAN_SIZE) break;
            after = tasks.get(tasks.size() - 1).getId();
        }
        after = null;
        while (true) {
            Query query = candidates(user, after, new Criteria().orOperator(
                    Criteria.where("tasks.source.connectionId").in(overrides.keySet()),
                    Criteria.where("tasks.target.connectionId").in(overrides.keySet())));
            query.fields().include("_id", "name", "status", "tasks", "user_id");
            List<InspectDto> inspections = inspectService.findAll(query);
            if (inspections == null || inspections.isEmpty()) break;
            for (InspectDto inspect : inspections) collect(report, inspectionTask(inspect), "inspect", overrides, user, skip, limit);
            if (inspections.size() < SCAN_SIZE) break;
            after = inspections.get(inspections.size() - 1).getId();
        }
        report.setHasConflicts(report.getTotal() > 0 || report.getHiddenCount() > 0);
        return report;
    }

    private Query candidates(UserDetail user, ObjectId after, Criteria reference) {
        Criteria criteria = Criteria.where("customId").is(user.getCustomerId()).and("is_deleted").ne(true)
                .and("status").nin(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED);
        if (after != null) criteria.and("_id").gt(after);
        return Query.query(new Criteria().andOperator(criteria, reference))
                .with(Sort.by(Sort.Direction.ASC, "_id")).limit(SCAN_SIZE);
    }

    private void collect(ConnectionAgentConflictReport report, TaskDto task, String type,
                         Map<String, DataSourceConnectionDto> overrides, UserDetail user, int skip, int limit) {
        try {
            taskScheduleService.validateConnectionAgentPolicy(task, user, overrides);
            return;
        } catch (BizException e) {
            if (!"Task.SourceAgentConflict".equals(e.getErrorCode()) && !"Task.AgentConflict".equals(e.getErrorCode())) throw e;
            if (!canView(task, type, user)) {
                report.setHiddenCount(report.getHiddenCount() + 1);
                return;
            }
            long index = report.getTotal();
            report.setTotal(index + 1);
            if (index < skip || report.getItems().size() >= limit) return;
            ConnectionAgentConflictReport.Item item = new ConnectionAgentConflictReport.Item();
            item.setId(task.getId().toHexString());
            item.setName(task.getName());
            item.setSyncType(type);
            item.setStatus(task.getStatus());
            item.setCode(e.getErrorCode());
            item.setReason(MessageUtil.getMessage("Task.AgentConflict"));
            item.setConnections(affectedConnections(task, overrides));
            report.getItems().add(item);
        }
    }

    protected boolean canView(TaskDto task, String type, UserDetail user) {
        DataPermissionMenuEnums menu = "inspect".equals(type) ? DataPermissionMenuEnums.INSPECT_TACK
                : DataPermissionMenuEnums.ofTaskSyncType(type);
        return menu != null && DataPermissionHelper.check(user, menu, DataPermissionActionEnums.View,
                menu.getDataType(), task.getId().toHexString(), () -> true, () -> false);
    }

    private List<ConnectionAgentConflictReport.Connection> affectedConnections(TaskDto task,
                                                                              Map<String, DataSourceConnectionDto> overrides) {
        Set<String> referenced = new HashSet<>();
        if (task.getDag() != null && task.getDag().getNodes() != null) {
            for (Node node : task.getDag().getNodes()) {
                if (node instanceof DataParentNode<?> data) referenced.add(data.getConnectionId());
                if (node instanceof LogCollectorNode log && log.getConnectionIds() != null) referenced.addAll(log.getConnectionIds());
            }
        }
        List<ConnectionAgentConflictReport.Connection> result = new ArrayList<>();
        overrides.forEach((id, dto) -> {
            if (referenced.contains(id)) {
                ConnectionAgentConflictReport.Connection connection = new ConnectionAgentConflictReport.Connection();
                connection.setId(id);
                connection.setName(dto.getName());
                result.add(connection);
            }
        });
        return result;
    }

    private TaskDto inspectionTask(InspectDto inspect) {
        TaskDto task = new TaskDto();
        task.setId(inspect.getId());
        task.setUserId(inspect.getUserId());
        task.setName(inspect.getName());
        task.setStatus(inspect.getStatus());
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        List<Node> nodes = new ArrayList<>();
        if (inspect.getTasks() != null) {
            for (com.tapdata.tm.inspect.bean.Task pair : inspect.getTasks()) {
                if (pair == null) continue;
                for (com.tapdata.tm.inspect.bean.Source source : Arrays.asList(pair.getSource(), pair.getTarget())) {
                    if (source == null || source.getConnectionId() == null) continue;
                    TableNode node = new TableNode();
                    node.setConnectionId(source.getConnectionId());
                    nodes.add(node);
                }
            }
        }
        Graph<Node, Edge> graph = new Graph<>();
        for (int i = 0; i < nodes.size(); i++) graph.setNode(Integer.toString(i), nodes.get(i));
        task.setDag(new DAG(graph));
        return task;
    }
}
