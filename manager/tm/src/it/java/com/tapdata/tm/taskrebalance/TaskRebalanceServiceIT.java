package com.tapdata.tm.taskrebalance;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsServiceImpl;
import com.tapdata.tm.agent.entity.AgentGroupEntity;
import com.tapdata.tm.agent.repository.AgentGroupRepository;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dblock.DBLockConfiguration;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceJobStatus;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceJobDto;
import com.tapdata.tm.taskrebalance.entity.TaskRebalanceEntity;
import com.tapdata.tm.taskrebalance.repository.TaskRebalanceRepository;
import com.tapdata.tm.taskrebalance.rule.TaskRebalanceRuleService;
import com.tapdata.tm.taskrebalance.service.TaskRebalanceJobService;
import com.tapdata.tm.taskrebalance.service.TaskRebalanceService;
import com.tapdata.tm.taskrebalance.vo.TaskRebalancePreviewVo;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** L2: real rule, rebalance and group services; storage and engine commands are boundaries. */
@Timeout(10)
class TaskRebalanceServiceIT {
    private GenericApplicationContext context;
    private TaskRebalanceService service;
    private TaskService tasks;
    private TaskRebalanceJobService jobs;
    private AgentGroupRepository groups;
    private WorkerService workers;
    private UserDetail user;
    private final Map<String, TaskDto> taskData = new LinkedHashMap<>();
    private List<String> members;
    private ObjectId rebalanceId;
    private final List<Update> jobUpdates = new ArrayList<>();

    @BeforeEach
    void setUp() {
        taskData.clear();
        jobUpdates.clear();
        members = List.of("a", "b");
        user = new UserDetail("user", "customer", "tester", "unused", List.of());
        user.setFreeAuth(true);
        rebalanceId = new ObjectId();
        tasks = mock(TaskService.class);
        jobs = mock(TaskRebalanceJobService.class);
        groups = mock(AgentGroupRepository.class);
        workers = mock(WorkerService.class);
        SettingsServiceImpl settings = mock(SettingsServiceImpl.class);
        DBLockConfiguration lock = mock(DBLockConfiguration.class);
        TaskRebalanceRepository repository = mock(TaskRebalanceRepository.class);
        when(lock.getOwner()).thenReturn("tm-it");
        TaskRebalanceEntity rebalance = new TaskRebalanceEntity();
        rebalance.setId(rebalanceId);
        rebalance.setExecuteOwner("tm-it");
        when(repository.findById(eq(rebalanceId), eq(user))).thenReturn(Optional.of(rebalance));
        when(workers.findAvailableAgentBySystem(any(List.class))).thenReturn(List.of(worker("a"), worker("b"), worker("c")));
        when(tasks.findAll(any(Query.class))).thenAnswer(invocation -> new ArrayList<>(taskData.values()));
        when(tasks.findByTaskId(any(ObjectId.class), any(String[].class))).thenAnswer(invocation ->
                taskData.get(invocation.<ObjectId>getArgument(0).toHexString()));
        when(groups.findAll(any(Query.class))).thenAnswer(invocation -> {
            assertThat(invocation.<Query>getArgument(0).getQueryObject().getString("groupId")).isEqualTo("g");
            AgentGroupEntity group = new AgentGroupEntity();
            group.setGroupId("g");
            group.setAgentIds(members);
            return List.of(group);
        });
        when(jobs.update(any(Query.class), any(Update.class), eq(user))).thenReturn(UpdateResult.acknowledged(1, 1L, null));
        doAnswer(invocation -> {
            jobUpdates.add(invocation.getArgument(1));
            return UpdateResult.acknowledged(1, 1L, null);
        }).when(jobs).updateById(any(ObjectId.class), any(Update.class), eq(user));
        doAnswer(invocation -> { invocation.<Runnable>getArgument(0).run(); return null; })
                .when(jobs).runAsRebalanceOperation(any(Runnable.class));

        // A fresh minimal Spring context per case avoids application/DB bootstrap and static fixtures.
        context = new GenericApplicationContext();
        context.registerBean(TaskRebalanceRuleService.class, TaskRebalanceRuleService::new);
        context.registerBean(AgentGroupService.class, () -> {
            AgentGroupService groupService = new AgentGroupService(groups);
            groupService.setSettingsService(settings);
            return groupService;
        });
        context.registerBean(TaskRebalanceService.class, () -> {
            TaskRebalanceService rebalanceService = new TaskRebalanceService(repository, jobs, tasks,
                    workers, mock(UserService.class), settings, lock, context.getBean(TaskRebalanceRuleService.class));
            ReflectionTestUtils.setField(rebalanceService, "agentGroupService", context.getBean(AgentGroupService.class));
            return rebalanceService;
        });
        context.refresh();
        service = context.getBean(TaskRebalanceService.class);
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
        taskData.clear();
    }

    @Test
    @DisplayName("一任务三节点：预览保留没有任务的在线节点和组内候选")
    void should_include_idle_online_agents() {
        addGroupTasks(1);
        TaskRebalancePreviewVo preview = service.preview(user);
        assertThat(preview.getAgentIds()).containsExactly("a", "b", "c");
        assertThat(preview.getTasks()).hasSize(1).allSatisfy(item ->
                assertThat(item.getAllowedAgentIds()).containsExactly("a", "b"));
    }

    @Test
    @DisplayName("无任务：预览仍返回全部在线节点")
    void should_include_agents_without_tasks() {
        assertThat(service.preview(user).getAgentIds()).containsExactly("a", "b", "c");
    }

    @Test
    @DisplayName("标签组预览：只向组内在线节点均衡")
    void should_preview_group_members() {
        addGroupTasks(4);
        TaskRebalancePreviewVo preview = service.preview(user);
        assertThat(preview.getMoveCount()).isEqualTo(2);
        assertThat(preview.getTasks()).allSatisfy(item -> {
            assertThat(item.getMovable()).isTrue();
            assertThat(item.getAllowedAgentIds()).containsExactly("a", "b");
            assertThat(item.getTargetAgentId()).isIn("a", "b");
        });
    }

    @Test
    @DisplayName("候选受限：跳过无法迁移的高优先级任务")
    void should_skip_blocked_candidate() {
        addGroupTasks(4);
        TaskDto pinnedGroup = taskData.values().iterator().next();
        pinnedGroup.setAccessNodeProcessId("single-member");
        when(groups.findAll(any(Query.class))).thenAnswer(invocation -> {
            AgentGroupEntity group = new AgentGroupEntity();
            String id = invocation.<Query>getArgument(0).getQueryObject().getString("groupId");
            group.setGroupId(id);
            group.setAgentIds("g".equals(id) ? members : List.of("a"));
            return List.of(group);
        });
        TaskRebalancePreviewVo preview = service.preview(user);
        assertThat(preview.getMoveCount()).isEqualTo(2);
        assertThat(preview.getTasks().get(0).getTargetAgentId()).isEqualTo("a");
    }

    @Test
    @DisplayName("组外目标提交：拒绝且不执行启停")
    void should_reject_outside_target() {
        addGroupTasks(4);
        TaskRebalancePreviewVo preview = changedPreview();
        preview.getTasks().get(0).setTargetAgentId("c");
        assertThatThrownBy(() -> service.createAndExecute(preview, user))
                .isInstanceOfSatisfying(BizException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo("task.rebalance.targetNotAllowed"));
        verify(tasks, never()).pause(any(TaskDto.class), any(), eq(false));
        verify(tasks, never()).start(any(TaskDto.class), any(), anyString());
    }

    @Test
    @DisplayName("预览后成员变化：提交重新校验")
    void should_revalidate_submission_members() {
        addGroupTasks(4);
        TaskRebalancePreviewVo preview = changedPreview();
        members = List.of("a");
        assertThatThrownBy(() -> service.createAndExecute(preview, user))
                .isInstanceOfSatisfying(BizException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo("task.rebalance.targetNotAllowed"));
        verify(tasks, never()).pause(any(TaskDto.class), any(), eq(false));
    }

    @Test
    @DisplayName("组内迁移：停止后在合法目标恢复运行")
    void should_execute_group_migration() {
        TaskDto task = addGroupTasks(1);
        doAnswer(invocation -> { task.setStatus(TaskDto.STATUS_STOP); return null; })
                .when(tasks).pause(eq(task), eq(user), eq(false));
        doAnswer(invocation -> {
            task.setAgentId(invocation.<Update>getArgument(1).getUpdateObject()
                    .get("$set", org.bson.Document.class).getString("agentId"));
            return UpdateResult.acknowledged(1, 1L, null);
        }).when(tasks).updateById(eq(task.getId()), any(Update.class), eq(user));
        doAnswer(invocation -> { task.setStatus(TaskDto.STATUS_RUNNING); return null; })
                .when(tasks).start(eq(task), eq(user), eq("11"));
        executeJob(task);
        assertThat(task.getAgentId()).isEqualTo("b");
        assertThat(task.getStatus()).isEqualTo(TaskDto.STATUS_RUNNING);
        verify(tasks).start(eq(task), eq(user), eq("11"));
        assertThat(jobUpdates).isNotEmpty();
        assertThat(jobUpdates.get(jobUpdates.size() - 1).getUpdateObject().get("$set", org.bson.Document.class)
                .getString(TaskRebalanceJobDto.FIELD_STATUS)).isEqualTo(TaskRebalanceJobStatus.OK);
    }

    @Test
    @DisplayName("执行前成员变化：拒绝停止源任务")
    void should_revalidate_before_stop() {
        TaskDto task = addGroupTasks(1);
        members = List.of("a");
        executeJob(task);
        assertInvalidAgent();
        verify(tasks, never()).pause(any(TaskDto.class), any(), eq(false));
        verify(tasks, never()).start(any(TaskDto.class), any(), anyString());
    }

    @Test
    @DisplayName("停止后成员变化：保留源节点且不启动目标")
    void should_revalidate_before_start() {
        TaskDto task = addGroupTasks(1);
        doAnswer(invocation -> {
            task.setStatus(TaskDto.STATUS_STOP);
            members = List.of("a");
            return null;
        }).when(tasks).pause(eq(task), eq(user), eq(false));
        executeJob(task);
        assertInvalidAgent();
        assertThat(task.getAgentId()).isEqualTo("a");
        assertThat(task.getStatus()).isEqualTo(TaskDto.STATUS_STOP);
        verify(tasks, never()).updateById(any(ObjectId.class), any(Update.class), any());
        verify(tasks, never()).start(any(TaskDto.class), any(), anyString());
    }

    @Test
    @DisplayName("空标签组：不能回退到全局节点")
    void should_reject_empty_group() {
        addGroupTasks(4);
        members = List.of();
        assertNoGroupMoves();
    }

    @Test
    @DisplayName("已删除标签组：不能回退到全局节点")
    void should_reject_deleted_group() {
        addGroupTasks(4);
        when(groups.findAll(any(Query.class))).thenReturn(List.of());
        assertNoGroupMoves();
    }

    @Test
    @DisplayName("组内候选离线：不能迁移到组外空闲节点")
    void should_ignore_offline_member() {
        addGroupTasks(4);
        when(workers.findAvailableAgentBySystem(any(List.class))).thenReturn(List.of(worker("a"), worker("c")));
        assertThat(service.preview(user).getMoveCount()).isZero();
    }

    @Test
    @DisplayName("指定单节点：保持不可均衡")
    void should_preserve_single_agent_pin() {
        addGroupTasks(4);
        taskData.values().forEach(task -> task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name()));
        TaskRebalancePreviewVo preview = service.preview(user);
        assertThat(preview.getMoveCount()).isZero();
        assertThat(preview.getTasks()).allSatisfy(item -> assertThat(item.getSchedulableStatus()).isEqualTo("MANUAL_AGENT"));
    }

    @Test
    @DisplayName("普通增量任务：继续正常均衡")
    void should_balance_unrestricted_tasks() {
        addGroupTasks(6);
        taskData.values().forEach(task -> { task.setAccessNodeType(null); task.setAccessNodeProcessIdList(null); });
        TaskRebalancePreviewVo preview = service.preview(user);
        assertThat(preview.getMoveCount()).isEqualTo(4);
        assertThat(preview.getTasks()).allSatisfy(item -> assertThat(item.getMovable()).isTrue());
    }

    private void assertNoGroupMoves() {
        TaskRebalancePreviewVo preview = service.preview(user);
        assertThat(preview.getMoveCount()).isZero();
        assertThat(preview.getTasks()).allSatisfy(item -> {
            assertThat(item.getMovable()).isFalse();
            assertThat(item.getSchedulableStatus()).isEqualTo("INVALID_AGENT_GROUP");
        });
    }

    private TaskRebalancePreviewVo changedPreview() {
        TaskRebalancePreviewVo preview = service.preview(user);
        preview.setTasks(preview.getTasks().stream().filter(item -> Boolean.TRUE.equals(item.getChanged())).toList());
        assertThat(preview.getTasks()).isNotEmpty();
        return preview;
    }

    private TaskDto addGroupTasks(int count) {
        TaskDto first = null;
        for (int i = 0; i < count; i++) {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setName("group-task-" + i);
            task.setStatus(TaskDto.STATUS_RUNNING);
            task.setType(ParentTaskDto.TYPE_CDC);
            task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
            task.setAgentId("a");
            task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            task.setAccessNodeProcessId("g");
            task.setAccessNodeProcessIdList(List.of("a", "b"));
            taskData.put(task.getId().toHexString(), task);
            if (first == null) { first = task; }
        }
        return first;
    }

    private void executeJob(TaskDto task) {
        TaskRebalanceJobDto job = new TaskRebalanceJobDto();
        job.setId(new ObjectId());
        job.setTaskId(task.getId().toHexString());
        job.setRebalanceId(rebalanceId.toHexString());
        job.setStatus(TaskRebalanceJobStatus.PENDING);
        job.setSourceAgentId("a");
        job.setTargetAgentId("b");
        when(jobs.findById(eq(job.getId()), eq(user))).thenReturn(job);
        // Exercise the synchronous job boundary without unrelated scheduler threads.
        ReflectionTestUtils.invokeMethod(service, "executeJob", rebalanceId.toHexString(), job, user,
                new AtomicBoolean(false), new AtomicReference<String>());
    }

    private void assertInvalidAgent() {
        assertThat(jobUpdates).isNotEmpty();
        assertThat(jobUpdates.get(jobUpdates.size() - 1).getUpdateObject().get("$set", org.bson.Document.class)
                .getString(TaskRebalanceJobDto.FIELD_STATUS)).isEqualTo(TaskRebalanceJobStatus.INVALID_AGENT);
    }

    private Worker worker(String id) {
        Worker worker = new Worker();
        worker.setProcessId(id);
        return worker;
    }
}
