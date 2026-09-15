package com.tapdata.tm.task.service.impl;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.utils.CacheUtils;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import com.tapdata.tm.utils.MessageUtil;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskDagCheckLogServiceImplLocalizationTest {

    @Test
    void localizesNodeNameWhenCreateLogFormatsAnEnglishTemplate() {
        TaskDagCheckLogServiceImpl service = new TaskDagCheckLogServiceImpl();
        TaskDagCheckLog log = service.createLog(
                "task-id",
                "node-id",
                "user-id",
                Level.INFO,
                DagOutputTemplateEnum.SOURCE_SETTING_CHECK,
                Locale.US,
                MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO"),
                MessageUtil.dagNodeName("database", "数据源节点"));

        assertTrue(log.getLog().contains("Data source node"));
    }

    @Test
    void createLogPreservesCustomNodeName() {
        TaskDagCheckLogServiceImpl service = new TaskDagCheckLogServiceImpl();
        TaskDagCheckLog log = service.createLog(
                "task-id",
                "node-id",
                "user-id",
                Level.INFO,
                DagOutputTemplateEnum.SOURCE_SETTING_CHECK,
                Locale.US,
                MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO"),
                "我的中文节点");

        assertTrue(log.getLog().contains("我的中文节点"));
        assertFalse(log.getLog().contains("Data source node"));
    }

    @Test
    void dagCheckPassesLocaleExplicitlyToCreateLog() {
        TaskDagCheckLogServiceImpl service = new TaskDagCheckLogServiceImpl();
        TaskDto taskDto = new TaskDto();
        taskDto.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        DAG dag = mock(DAG.class);
        when(dag.checkMultiDag()).thenReturn(false);
        taskDto.setDag(dag);
        UserDetail user = mock(UserDetail.class);

        DagLogStrategy strategy = (dto, userDetail, locale) -> List.of(service.createLog(
                "task-id",
                "node-id",
                "user-id",
                Level.INFO,
                DagOutputTemplateEnum.SOURCE_SETTING_CHECK,
                locale,
                MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_INFO"),
                MessageUtil.dagNodeName("database", "数据源节点")));

        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(() -> SpringUtil.getBean(anyString(), eq(DagLogStrategy.class))).thenReturn(strategy);

            List<TaskDagCheckLog> chineseLogs = service.dagCheck(taskDto, user, false, Locale.CHINA);
            assertTrue(chineseLogs.stream().anyMatch(log -> log.getLog().contains("数据源节点")));
            assertTrue(chineseLogs.stream().noneMatch(log -> log.getLog().contains("Data source node")));

            List<TaskDagCheckLog> englishLogs = service.dagCheck(taskDto, user, false, Locale.US);
            assertTrue(englishLogs.stream().anyMatch(log -> log.getLog().contains("Data source node")));
        }

        TaskDagCheckLog afterCheck = service.createLog(
                "task-id",
                "node-id",
                "user-id",
                Level.INFO,
                DagOutputTemplateEnum.SOURCE_SETTING_CHECK,
                Locale.US,
                MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO"),
                MessageUtil.dagNodeName("database", "数据源节点"));
        assertTrue(afterCheck.getLog().contains("Data source node"));
    }

    @Test
    void getLogsLocalizesDefaultNodeNamesInNodeMapAndPreservesCustomNames() {
        TaskDagCheckLogServiceImpl service = new TaskDagCheckLogServiceImpl();
        TaskService taskService = mock(TaskService.class);
        ReflectionTestUtils.setField(service, "taskService", taskService);

        String taskId = new ObjectId().toHexString();
        TableNode defaultNode = new TableNode();
        defaultNode.setId("default-node");
        defaultNode.setName("表节点");
        TableNode customNode = new TableNode();
        customNode.setId("custom-node");
        customNode.setName("数据源节点");

        DAG dag = mock(DAG.class);
        when(dag.getNodes()).thenReturn(List.of(defaultNode, customNode));

        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId(taskId));
        taskDto.setDag(dag);
        taskDto.setTransformed(false);
        when(taskService.findById(any())).thenReturn(taskDto);

        TaskLogDto dto = new TaskLogDto();
        dto.setTaskId(taskId);

        try (MockedStatic<CacheUtils> cache = mockStatic(CacheUtils.class)) {
            cache.when(() -> CacheUtils.isExist(any())).thenReturn(true);
            cache.when(() -> CacheUtils.get(any())).thenReturn(Collections.emptyList());

            TaskDagCheckLogVo vo = service.getLogs(dto, mock(UserDetail.class), Locale.US);

            assertEquals("Table node", vo.getNodes().get("default-node"));
            assertEquals("数据源节点", vo.getNodes().get("custom-node"));
        }
    }

    @Test
    void getLogsPassesLocalizedJsTypeNameToMonitoringLogs() {
        verifyJsNodeLogName("增强JS节点", "Enhanced JS node");
    }

    @Test
    void getLogsPassesCustomJsNodeNameToMonitoringLogs() {
        verifyJsNodeLogName("我的JS", "我的JS");
    }

    private void verifyJsNodeLogName(String storedName, String expectedName) {
        TaskDagCheckLogServiceImpl service = new TaskDagCheckLogServiceImpl();
        TaskService taskService = mock(TaskService.class);
        MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
        ReflectionTestUtils.setField(service, "taskService", taskService);
        ReflectionTestUtils.setField(service, "monitoringLogsService", monitoringLogsService);

        String taskId = new ObjectId().toHexString();
        JsProcessorNode jsNode = new JsProcessorNode();
        jsNode.setId("js-node");
        jsNode.setName(storedName);

        DAG dag = mock(DAG.class);
        when(dag.getNodes()).thenReturn(List.of(jsNode));

        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId(taskId));
        taskDto.setDag(dag);
        taskDto.setTransformed(false);
        taskDto.setName("task-name");
        taskDto.setTransformTaskId("transform-id");
        when(taskService.findById(any())).thenReturn(taskDto);
        when(monitoringLogsService.getJsNodeLog(any(), any(), any(), any())).thenReturn(Collections.emptyList());

        TaskLogDto dto = new TaskLogDto();
        dto.setTaskId(taskId);

        try (MockedStatic<CacheUtils> cache = mockStatic(CacheUtils.class)) {
            cache.when(() -> CacheUtils.isExist(any())).thenReturn(true);
            cache.when(() -> CacheUtils.get(any())).thenReturn(Collections.emptyList());

            service.getLogs(dto, mock(UserDetail.class), Locale.US);
        }

        verify(monitoringLogsService).getJsNodeLog("transform-id", "task-name", expectedName, Locale.US);
    }
}
