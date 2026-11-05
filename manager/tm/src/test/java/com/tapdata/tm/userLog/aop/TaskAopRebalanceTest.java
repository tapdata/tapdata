package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.taskrebalance.repository.TaskRebalanceJobRepository;
import com.tapdata.tm.taskrebalance.service.TaskRebalanceJobService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import org.aspectj.lang.JoinPoint;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that TaskAop routes operation logs to REBALANCE_START / REBALANCE_STOP
 * when invoked inside {@link TaskRebalanceJobService#runAsRebalanceOperation},
 * and to ordinary START / STOP / FORCE_STOP otherwise. This locks in the assumption
 * that AOP advice runs synchronously on the same thread as the advised call,
 * so the ThreadLocal flag carries the rebalance context correctly.
 */
class TaskAopRebalanceTest {

    private TaskAop aop;
    private TaskService taskService;
    private UserLogService userLogService;
    private TaskRebalanceJobService taskRebalanceJobService;

    private final ObjectId taskId = new ObjectId();

    @BeforeEach
    void setUp() {
        aop = new TaskAop();
        taskService = mock(TaskService.class);
        userLogService = mock(UserLogService.class);
        taskRebalanceJobService = new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class));
        aop.setTaskService(taskService);
        aop.setUserLogService(userLogService);
        aop.setTaskRebalanceJobService(taskRebalanceJobService);
    }

    private TaskDto syncTask() {
        TaskDto task = new TaskDto();
        task.setId(taskId);
        task.setName("t1");
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        return task;
    }

    private JoinPoint pauseJoinPoint(boolean force) {
        UserDetail user = mock(UserDetail.class);
        JoinPoint jp = mock(JoinPoint.class);
        when(jp.getArgs()).thenReturn(new Object[]{taskId, user, force});
        when(taskService.checkExistById(eq(taskId), any(UserDetail.class))).thenReturn(syncTask());
        return jp;
    }

    private JoinPoint startJoinPoint() {
        UserDetail user = mock(UserDetail.class);
        JoinPoint jp = mock(JoinPoint.class);
        when(jp.getArgs()).thenReturn(new Object[]{taskId, user, "11"});
        when(taskService.checkExistById(eq(taskId), any(UserDetail.class))).thenReturn(syncTask());
        return jp;
    }

    @Test
    @DisplayName("normal pause produces Operation.STOP")
    void normalPauseProducesStop() {
        aop.afterStoppedPointcut(pauseJoinPoint(false));
        verify(userLogService, times(1)).addUserLog(
                eq(Modular.SYNC), eq(Operation.STOP), any(UserDetail.class),
                eq(taskId.toString()), eq("t1"), any());
    }

    @Test
    @DisplayName("force pause outside rebalance produces Operation.FORCE_STOP")
    void forcePauseProducesForceStop() {
        aop.afterStoppedPointcut(pauseJoinPoint(true));
        verify(userLogService, times(1)).addUserLog(
                eq(Modular.SYNC), eq(Operation.FORCE_STOP), any(UserDetail.class),
                eq(taskId.toString()), eq("t1"), any());
    }

    @Test
    @DisplayName("normal start produces Operation.START")
    void normalStartProducesStart() {
        aop.afterStartPointcut(startJoinPoint());
        verify(userLogService, times(1)).addUserLog(
                eq(Modular.SYNC), eq(Operation.START), any(UserDetail.class),
                eq(taskId.toString()), eq("t1"), any());
    }

    @Test
    @DisplayName("pause inside runAsRebalanceOperation produces Operation.REBALANCE_STOP")
    void pauseInsideRebalanceProducesRebalanceStop() {
        JoinPoint jp = pauseJoinPoint(false);
        taskRebalanceJobService.runAsRebalanceOperation(() -> aop.afterStoppedPointcut(jp));
        verify(userLogService, times(1)).addUserLog(
                eq(Modular.SYNC), eq(Operation.REBALANCE_STOP), any(UserDetail.class),
                eq(taskId.toString()), eq("t1"), any());
    }

    @Test
    @DisplayName("force pause inside runAsRebalanceOperation still produces REBALANCE_STOP")
    void forcePauseInsideRebalanceStillProducesRebalanceStop() {
        JoinPoint jp = pauseJoinPoint(true);
        taskRebalanceJobService.runAsRebalanceOperation(() -> aop.afterStoppedPointcut(jp));
        verify(userLogService, times(1)).addUserLog(
                eq(Modular.SYNC), eq(Operation.REBALANCE_STOP), any(UserDetail.class),
                eq(taskId.toString()), eq("t1"), any());
    }

    @Test
    @DisplayName("start inside runAsRebalanceOperation produces Operation.REBALANCE_START")
    void startInsideRebalanceProducesRebalanceStart() {
        JoinPoint jp = startJoinPoint();
        taskRebalanceJobService.runAsRebalanceOperation(() -> aop.afterStartPointcut(jp));
        verify(userLogService, times(1)).addUserLog(
                eq(Modular.SYNC), eq(Operation.REBALANCE_START), any(UserDetail.class),
                eq(taskId.toString()), eq("t1"), any());
    }
}
