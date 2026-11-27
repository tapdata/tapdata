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
import com.tapdata.tm.ds.dto.ConnectionTaskAgentRequest;
import com.tapdata.tm.ds.dto.ConnectionTaskAgentResult;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.task.entity.TaskEntity;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;
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

/** Checks proposed connections and saves explicitly requested task policy edits separately. */
@Service
@Setter
public class ConnectionAgentConflictService {
    private static final int SCAN_SIZE = 200;
    private static final String ACCESS_NODE_PROCESS_ID = "accessNodeProcessId";
    private static final String ACCESS_NODE_PROCESS_ID_LIST = "accessNodeProcessIdList";
    private static final String PRIORITY_PROCESS_ID = "priorityProcessId";
    private static final String LAST_UPD_AT = "lastUpdAt";
    @Autowired private MongoTemplate mongoTemplate;
    @Autowired @Lazy private DataSourceService dataSourceService;
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
                    ACCESS_NODE_PROCESS_ID, ACCESS_NODE_PROCESS_ID_LIST, PRIORITY_PROCESS_ID, "user_id", "shareCache", LAST_UPD_AT);
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

    /** Explicitly saves task policies first. Connection settings are saved separately. */
    public ConnectionTaskAgentResult updateTaskAgents(Map<String, DataSourceConnectionDto> overrides,
                                                     ConnectionTaskAgentRequest request, UserDetail user) {
        if (request.getTaskEdits() == null || request.getTaskEdits().isEmpty()
                || request.getTaskEdits().size() > 100 || request.getTaskSettings() == null) {
            throw new BizException("IllegalArgument", "Select 1-100 tasks and an Agent policy");
        }
        BatchPolicy policy = new BatchPolicy(request.getTaskSettings());
        Set<String> unique = new HashSet<>();
        for (ConnectionTaskAgentRequest.TaskEdit edit : request.getTaskEdits()) {
            if (edit == null || edit.getId() == null || !ObjectId.isValid(edit.getId()) || !unique.add(edit.getId())) {
                throw new BizException("IllegalArgument", "Invalid or duplicate task id");
            }
        }
        ConnectionTaskAgentResult result = new ConnectionTaskAgentResult();
        for (ConnectionTaskAgentRequest.TaskEdit edit : request.getTaskEdits()) {
            try {
                Query query = Query.query(Criteria.where("_id").is(new ObjectId(edit.getId()))
                        .and("customId").is(user.getCustomerId()).and("is_deleted").ne(true));
                List<TaskDto> found = taskService.findAll(query);
                TaskDto task = found == null || found.size() != 1 ? null : found.get(0);
                if (task == null || !task.usesConnectionAgentPolicy() || !canView(task, task.getSyncType(), user)
                        || !canEdit(task, user)) throw new BizException("Datasource.NotFound");
                if (!editableStatus(task.getStatus())) throw new BizException("IllegalArgument", "Task must be stopped");
                Long revision = task.getLastUpdAt() == null ? null : task.getLastUpdAt().getTime();
                if (!Objects.equals(revision, edit.getRevision())) throw new BizException("IllegalArgument", "Task changed; refresh conflicts");
                if (affectedConnections(task, overrides).isEmpty()) throw new BizException("IllegalArgument", "Task does not reference these connections");
                TaskDto candidate = new TaskDto();
                BeanUtils.copyProperties(task, candidate, ACCESS_NODE_PROCESS_ID_LIST);
                candidate.setAccessNodeType(policy.type);
                candidate.setAccessNodeProcessId(policy.processId);
                candidate.setPriorityProcessId(policy.priority);
                // Tasks are persisted before the connection update. Keep the new policy
                // valid if the user cancels or the subsequent connection write fails.
                validateTaskAgentPolicy(candidate, user, overrides);
                query.addCriteria(Criteria.where("status").is(task.getStatus()).and(LAST_UPD_AT).is(task.getLastUpdAt())
                        .and("accessNodeType").is(task.getAccessNodeType())
                        .and(ACCESS_NODE_PROCESS_ID).is(task.getAccessNodeProcessId())
                        .and(PRIORITY_PROCESS_ID).is(task.getPriorityProcessId()));
                Update update = new Update().set("accessNodeType", policy.type).set(ACCESS_NODE_PROCESS_ID, policy.processId)
                        .set(PRIORITY_PROCESS_ID, policy.priority)
                        .set(ACCESS_NODE_PROCESS_ID_LIST, AccessNodeTypeEnum.isUserManually(policy.type)
                                ? Collections.singletonList(policy.processId) : Collections.emptyList())
                        .set(LAST_UPD_AT, new Date()).set("lastUpdBy", user.getUserId());
                if (task.getAttrs() != null) {
                    update.set("attrs." + TaskDto.ATTRS_SOURCE_AGENT_POLICY_INHERITED, false);
                }
                if (mongoTemplate.updateFirst(query, update, TaskEntity.class).getMatchedCount() != 1) {
                    throw new BizException("IllegalArgument", "Task changed; refresh conflicts");
                }
                result.getUpdatedTaskIds().add(edit.getId());
            } catch (BizException e) {
                ConnectionTaskAgentResult.Failure failure = new ConnectionTaskAgentResult.Failure();
                failure.setId(edit.getId()); failure.setCode(e.getErrorCode()); result.getFailures().add(failure);
            } catch (org.springframework.dao.DataAccessException e) {
                ConnectionTaskAgentResult.Failure failure = new ConnectionTaskAgentResult.Failure();
                failure.setId(edit.getId()); failure.setCode("Task.AgentUpdateFailed"); result.getFailures().add(failure);
            }
        }
        return result;
    }

    protected boolean canEdit(TaskDto task, UserDetail user) {
        DataPermissionMenuEnums menu = DataPermissionMenuEnums.ofTaskSyncType(task.getSyncType());
        return menu != null && DataPermissionHelper.check(user, menu, DataPermissionActionEnums.Edit,
                menu.getDataType(), task.getId().toHexString(), () -> true, () -> false);
    }

    static boolean editableStatus(String status) {
        return Arrays.asList("edit", "ready", "stop", "wait_start", "stopped").contains(status);
    }

    private static class BatchPolicy {
        final String type, processId, priority;
        BatchPolicy(com.tapdata.tm.ds.dto.BatchUpdateAgentSettingsRequest.AgentSettings settings) {
            type = settings.getAccessNodeType();
            if (!AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name().equals(type)
                    && !AccessNodeTypeEnum.isManually(type)) throw new BizException("IllegalArgument", "Invalid Agent policy");
            processId = AccessNodeTypeEnum.isManually(type) ? settings.getAccessNodeProcessId() : null;
            priority = AccessNodeTypeEnum.isGroupManually(type) ? settings.getPriorityProcessId() : null;
            if (AccessNodeTypeEnum.isManually(type) && (processId == null || processId.isBlank())) {
                throw new BizException("IllegalArgument", "Agent or group is required");
            }
        }
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
            item.setRevision(task.getLastUpdAt() == null ? null : task.getLastUpdAt().getTime());
            boolean permission = "inspect".equals(type) ? canEditInspection(task, user) : canEdit(task, user);
            item.setCanEdit(permission);
            item.setEditable(!"inspect".equals(type) && editableStatus(task.getStatus()) && permission);
            boolean connectionConflict = "Task.SourceAgentConflict".equals(e.getErrorCode());
            if (item.isEditable() && referencedConnections(task).stream().allMatch(id -> canViewConnection(id, user))) {
                List<List<String>> allowedAgentSets = new ArrayList<>(2);
                Set<String> currentAgents = taskScheduleService.connectionAllowedAgents(task, user, Collections.emptyMap());
                Set<String> proposedAgents = taskScheduleService.connectionAllowedAgents(task, user, overrides);
                if (currentAgents != null) allowedAgentSets.add(new ArrayList<>(currentAgents));
                if (proposedAgents != null) allowedAgentSets.add(new ArrayList<>(proposedAgents));
                item.setAllowedAgentProcessIdSets(allowedAgentSets);

            }
            if (connectionConflict) item.setEditable(false);
            item.setAutoResolvable(item.isEditable() && automaticResolves(task, overrides, user));
            item.setBlockedReason(!permission ? "no_edit_permission"
                    : connectionConflict ? "connection_conflict"
                    : "inspect".equals(type) ? "unsupported_policy"
                    : isActiveStatus(task.getStatus()) ? "task_active"
                    : item.isAutoResolvable() ? null : "policy_conflict");
            item.setCode(e.getErrorCode());
            item.setReason(MessageUtil.getMessage("Task.AgentConflict"));
            item.setConnections(connectionConflict
                    ? conflictConnections(task, overrides, user) : affectedConnections(task, overrides));
            report.getItems().add(item);
        }
    }

    protected boolean automaticResolves(TaskDto task, Map<String, DataSourceConnectionDto> overrides, UserDetail user) {
        TaskDto automatic = new TaskDto();
        BeanUtils.copyProperties(task, automatic, ACCESS_NODE_PROCESS_ID_LIST);
        automatic.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
        automatic.setAccessNodeProcessId(null);
        automatic.setPriorityProcessId(null);
        try {
            validateTaskAgentPolicy(automatic, user, overrides);
            return true;
        } catch (BizException e) {
            if ("Task.SourceAgentConflict".equals(e.getErrorCode()) || "Task.AgentConflict".equals(e.getErrorCode())) return false;
            throw e;
        }
    }

    private void validateTaskAgentPolicy(TaskDto task, UserDetail user,
                                         Map<String, DataSourceConnectionDto> overrides) {
        try {
            taskScheduleService.validateConnectionAgentPolicy(task, user, Collections.emptyMap());
        } catch (BizException e) {
            // Automatic allocation can repair an already unusable current policy.
            if (!AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name().equals(task.getAccessNodeType())
                    || !"Task.SourceAgentConflict".equals(e.getErrorCode())) throw e;
        }
        taskScheduleService.validateConnectionAgentPolicy(task, user, overrides);
    }

    private boolean isActiveStatus(String status) {
        return Arrays.asList("preparing", "scheduling", "wait_run", "starting", "running", "stopping", "renewing").contains(status);
    }

    protected boolean canEditInspection(TaskDto task, UserDetail user) {
        DataPermissionMenuEnums menu = DataPermissionMenuEnums.INSPECT_TACK;
        return DataPermissionHelper.check(user, menu, DataPermissionActionEnums.Edit,
                menu.getDataType(), task.getId().toHexString(), () -> true, () -> false);
    }

    protected boolean canViewConnection(String id, UserDetail user) {
        return DataPermissionHelper.check(user, DataPermissionMenuEnums.Connections,
                DataPermissionActionEnums.View, DataPermissionMenuEnums.Connections.getDataType(), id,
                () -> true, () -> false);
    }

    private List<ConnectionAgentConflictReport.Connection> conflictConnections(TaskDto task,
            Map<String, DataSourceConnectionDto> overrides, UserDetail user) {
        Map<String, DataSourceConnectionDto> all = new LinkedHashMap<>(overrides);
        Set<String> ids = new LinkedHashSet<>();
        if (task.getDag() != null && task.getDag().getNodes() != null) {
            for (Node node : task.getDag().getNodes()) {
                if (node instanceof DataParentNode<?> data && data.getConnectionId() != null) ids.add(data.getConnectionId());
                if (node instanceof LogCollectorNode log && log.getConnectionIds() != null) ids.addAll(log.getConnectionIds());
            }
        }
        if (dataSourceService != null && !ids.isEmpty()) {
            Query query = Query.query(Criteria.where("_id").in(ids));
            query.fields().include("_id", "name");
            List<DataSourceConnectionDto> connections = dataSourceService.findAll(query);
            if (connections != null) for (DataSourceConnectionDto connection : connections) {
                if (connection.getId() == null) continue;
                String id = connection.getId().toHexString();
                if (all.containsKey(id)) continue;
                boolean visible = canViewConnection(id, user);
                if (visible) all.put(id, connection);
            }
        }
        return affectedConnections(task, all);
    }

    protected boolean canView(TaskDto task, String type, UserDetail user) {
        DataPermissionMenuEnums menu = "inspect".equals(type) ? DataPermissionMenuEnums.INSPECT_TACK
                : DataPermissionMenuEnums.ofTaskSyncType(type);
        return menu != null && DataPermissionHelper.check(user, menu, DataPermissionActionEnums.View,
                menu.getDataType(), task.getId().toHexString(), () -> true, () -> false);
    }

    private List<ConnectionAgentConflictReport.Connection> affectedConnections(TaskDto task,
                                                                              Map<String, DataSourceConnectionDto> overrides) {
        Set<String> referenced = referencedConnections(task);
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

    private Set<String> referencedConnections(TaskDto task) {
        Set<String> referenced = new HashSet<>();
        if (task.getDag() != null && task.getDag().getNodes() != null) {
            for (Node node : task.getDag().getNodes()) {
                if (node instanceof DataParentNode<?> data) referenced.add(data.getConnectionId());
                if (node instanceof LogCollectorNode log && log.getConnectionIds() != null) referenced.addAll(log.getConnectionIds());
            }
        }
        referenced.removeIf(id -> id == null || id.isBlank());
        return referenced;
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
