package com.tapdata.tm.task.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.repository.TaskRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Real task start with a DAG parsed by the production node deserializer; storage is the external boundary. */
@Timeout(10)
class TaskUnknownNodeStartIT {

    @SafeVarargs
    private static DAG dag(Map<String, Object>... nodes) {
        String json = JsonUtil.toJsonUseJackson(Map.of("nodes", List.of(nodes), "edges", List.of()));
        return DAG.build(JsonUtil.parseJsonUseJackson(json, Dag.class));
    }

    private static TaskDto task(DAG dag) {
        TaskDto task = new TaskDto();
        task.setName("task left by a dev build");
        task.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        task.setType("initial_sync");
        task.setStatus(TaskDto.STATUS_EDIT);
        task.setDag(dag);
        return task;
    }

    @Test
    @DisplayName("L2：启动含当前版本未注册节点类型的任务，在任何持久化之前以 Task.UnknownNodeType 拒绝")
    void shouldRejectUnknownNodeBeforePersistingStart() {
        TaskRepository tasks = mock(TaskRepository.class);
        TaskServiceImpl service = new TaskServiceImpl(tasks);
        TaskDto task = task(dag(
                Map.of("id", "table-1", "type", "table", "tableName", "orders"),
                Map.of("id", "unknown-2", "type", "dev_z_processor"),
                Map.of("id", "unknown-1", "type", "dev_a_processor", "devScript", "return record;")));

        BizException error = assertThrows(BizException.class, () -> service.start(task, mock(UserDetail.class), "11"));

        assertEquals("Task.UnknownNodeType", error.getErrorCode());
        assertArrayEquals(new Object[]{"dev_a_processor, dev_z_processor"}, error.getArgs());
        assertNull(task.getLastStartDate());
        verifyNoInteractions(tasks);
    }

    @Test
    @DisplayName("L2：只含已注册节点类型的任务不触发未知节点校验")
    void shouldAcceptTaskWithRegisteredNodesOnly() {
        TaskServiceImpl service = new TaskServiceImpl(mock(TaskRepository.class));
        TaskDto task = task(dag(Map.of("id", "table-1", "type", "table", "tableName", "orders")));

        assertDoesNotThrow(() -> service.validateUnknownNodes(task));
    }
}
