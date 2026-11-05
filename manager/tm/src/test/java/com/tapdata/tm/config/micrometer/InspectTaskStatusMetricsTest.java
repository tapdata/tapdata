package com.tapdata.tm.config.micrometer;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.inspect.service.InspectTaskService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for InspectTaskStatusMetrics
 * 
 * @author lg<lirufei0808@gmail.com>
 */
@ExtendWith(MockitoExtension.class)
class InspectTaskStatusMetricsTest {

    @Mock
    private InspectTaskService inspectTaskService;

    @Mock
    private BaseService baseService;

    @InjectMocks
    private InspectTaskStatusMetrics inspectTaskStatusMetrics;

    private MeterRegistry originalRegistry;
    private SimpleMeterRegistry testRegistry;

    @BeforeEach
    void setUp() {
        // 保存原始全局 registry
        originalRegistry = Metrics.globalRegistry.getRegistries().isEmpty()
                ? null
                : Metrics.globalRegistry.getRegistries().iterator().next();

        // 用可控的 SimpleMeterRegistry 替换全局（测试隔离）
        testRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(testRegistry);
        if (originalRegistry != null) {
            Metrics.globalRegistry.remove(originalRegistry);
        }
    }

    @AfterEach
    void tearDown() {
        // 恢复原始 registry
        if (originalRegistry != null) {
            Metrics.globalRegistry.add(originalRegistry);
        }
        Metrics.globalRegistry.remove(testRegistry);
    }

    @Test
    @DisplayName("构造函数应该接受 InspectTaskService 并转换为 BaseService")
    void constructor_shouldAcceptInspectTaskService() {
        // InspectTaskService 接口本身不是 BaseService 的实例，taskService 应该为 null
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(inspectTaskService);
        
        Map<String, ?> gauges = getPrivateFieldTaskGauges(metrics);
        assertThat(gauges).isNotNull();
        assertThat(gauges).isEmpty();
    }

    @Test
    @DisplayName("构造函数应该正确处理 BaseService 实例")
    void constructor_shouldHandleBaseServiceInstance() {
        // 当 InspectTaskService 也是 BaseService 实例时（如 InspectTaskServiceImpl）
        // 模拟一个既是 InspectTaskService 又是 BaseService 的对象
        BaseService mockBaseService = mock(BaseService.class);
        
        // 由于我们无法直接创建既是 InspectTaskService 又是 BaseService 的 mock，
        // 我们通过反射直接设置 taskService 字段来测试
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        setPrivateFieldTaskService(metrics, mockBaseService);
        
        Map<String, ?> gauges = getPrivateFieldTaskGauges(metrics);
        assertThat(gauges).isNotNull();
    }

    @Test
    @DisplayName("应该正确映射 PASSED 状态到编码 1")
    void mapStatusToCode_shouldReturn1ForPassed() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode("passed")).isEqualTo(1);
    }

    @Test
    @DisplayName("应该正确映射 ERROR 状态到编码 2")
    void mapStatusToCode_shouldReturn2ForError() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode("error")).isEqualTo(2);
    }

    @Test
    @DisplayName("应该正确映射 RUNNING 状态到编码 3")
    void mapStatusToCode_shouldReturn3ForRunning() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode("running")).isEqualTo(3);
    }

    @Test
    @DisplayName("应该正确映射 FAILED 状态到编码 4")
    void mapStatusToCode_shouldReturn4ForFailed() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode("failed")).isEqualTo(4);
    }

    @Test
    @DisplayName("应该正确映射 DONE 状态到编码 5")
    void mapStatusToCode_shouldReturn5ForDone() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode("done")).isEqualTo(5);
    }

    @Test
    @DisplayName("应该正确映射 WAITING 状态到编码 6")
    void mapStatusToCode_shouldReturn6ForWaiting() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode("waiting")).isEqualTo(6);
    }

    @Test
    @DisplayName("应该正确映射 SCHEDULING 状态到编码 7")
    void mapStatusToCode_shouldReturn7ForScheduling() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode("scheduling")).isEqualTo(7);
    }

    @Test
    @DisplayName("应该正确映射 STOPPING 状态到编码 8")
    void mapStatusToCode_shouldReturn8ForStopping() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode("stopping")).isEqualTo(8);
    }

    @Test
    @DisplayName("当状态为 null 时应该返回编码 0")
    void mapStatusToCode_shouldReturn0ForNullStatus() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode(null)).isEqualTo(0);
    }

    @Test
    @DisplayName("当状态未知时应该返回编码 0")
    void mapStatusToCode_shouldReturn0ForUnknownStatus() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        assertThat(metrics.mapStatusToCode("unknown_status")).isEqualTo(0);
    }

    @Test
    @DisplayName("应该正确处理空字符串状态")
    void mapStatusToCode_shouldHandleEmptyStatus() {
        InspectTaskStatusMetrics metrics = new InspectTaskStatusMetrics(null);
        // 空字符串不会被 InspectStatusEnum.of 识别，应该返回 0
        assertThat(metrics.mapStatusToCode("")).isEqualTo(0);
    }

    @Test
    @DisplayName("当数据库返回 null 时应该安全处理")
    void refreshTaskStatuses_shouldHandleNullResultFromDatabase() {
        // 使用反射设置 taskService 为 mock 的 baseService
        setPrivateFieldTaskService(inspectTaskStatusMetrics, baseService);
        
        when(baseService.findAllEntity(any(Query.class))).thenReturn(null);
        
        // 不应该抛出异常
        inspectTaskStatusMetrics.refreshTaskStatuses();
        
        verify(baseService).findAllEntity(any(Query.class));
    }

    @Test
    @DisplayName("当数据库返回空列表时应该正确处理")
    void refreshTaskStatuses_shouldHandleEmptyListFromDatabase() {
        setPrivateFieldTaskService(inspectTaskStatusMetrics, baseService);
        
        when(baseService.findAllEntity(any(Query.class))).thenReturn(Collections.emptyList());
        
        inspectTaskStatusMetrics.refreshTaskStatuses();
    }

    @Test
    @DisplayName("当新任务出现时应该创建对应的 Gauge")
    void refreshTaskStatuses_shouldCreateGaugesForNewTasks() {
        setPrivateFieldTaskService(inspectTaskStatusMetrics, baseService);

        InspectEntity task1 = createInspectEntity("69902630d652589d497a8ab1", "running", "Test Task 1");
        InspectEntity task2 = createInspectEntity("69902630d652589d497a8ab2", "error", "Test Task 2");

        when(baseService.findAllEntity(any(Query.class))).thenReturn(List.of(task1, task2));

        inspectTaskStatusMetrics.refreshTaskStatuses();

        // 验证每个 gauge 的值和标签
        assertGaugeValueAndTags("69902630d652589d497a8ab1", "Test Task 1", 3);
        assertGaugeValueAndTags("69902630d652589d497a8ab2", "Test Task 2", 2);
    }

    @Test
    @DisplayName("应该跳过 ID 为 null 的任务")
    void refreshTaskStatuses_shouldSkipTasksWithNullId() {
        setPrivateFieldTaskService(inspectTaskStatusMetrics, baseService);
        
        InspectEntity taskWithNullId = new InspectEntity();
        taskWithNullId.setId(null);
        taskWithNullId.setStatus("running");
        taskWithNullId.setName("Task with null ID");
        
        InspectEntity validTask = createInspectEntity("69902630d652589d497a8ab3", "passed", "Valid Task");
        
        when(baseService.findAllEntity(any(Query.class))).thenReturn(List.of(taskWithNullId, validTask));
        
        inspectTaskStatusMetrics.refreshTaskStatuses();
        
        // 只应该为有效任务创建 gauge
        assertThat(testRegistry.find("inspect_task_status").gauges()).hasSize(1);
        assertGaugeValueAndTags("69902630d652589d497a8ab3", "Valid Task", 1);
    }

    @Test
    @DisplayName("应该更新已存在任务的状态编码")
    void refreshTaskStatuses_shouldUpdateStatusCodeForExistingTasks() {
        setPrivateFieldTaskService(inspectTaskStatusMetrics, baseService);
        
        String taskId = "69902630d652589d497a8ab7";
        InspectEntity task = createInspectEntity(taskId, "running", "Updating Task");
        
        // 第一次：running 状态
        when(baseService.findAllEntity(any(Query.class))).thenReturn(List.of(task));
        inspectTaskStatusMetrics.refreshTaskStatuses();
        assertGaugeValueAndTags(taskId, "Updating Task", 3);
        
        // 第二次：状态变为 error
        task.setStatus("error");
        inspectTaskStatusMetrics.refreshTaskStatuses();
        assertGaugeValueAndTags(taskId, "Updating Task", 2);
        
        // 第三次：状态变为 done
        task.setStatus("done");
        inspectTaskStatusMetrics.refreshTaskStatuses();
        assertGaugeValueAndTags(taskId, "Updating Task", 5);
    }

    @Test
    @DisplayName("应该正确处理所有状态枚举值")
    void refreshTaskStatuses_shouldHandleAllStatusEnumValues() {
        setPrivateFieldTaskService(inspectTaskStatusMetrics, baseService);
        
        String[] statuses = {"passed", "error", "running", "failed", "done", "waiting", "scheduling", "stopping"};
        int[] expectedCodes = {1, 2, 3, 4, 5, 6, 7, 8};
        
        for (int i = 0; i < statuses.length; i++) {
            String taskId = "69902630d652589d497a000" + i;
            InspectEntity task = createInspectEntity(taskId, statuses[i], "Task " + statuses[i]);
            
            when(baseService.findAllEntity(any(Query.class))).thenReturn(List.of(task));
            inspectTaskStatusMetrics.refreshTaskStatuses();
            
            assertGaugeValueAndTags(taskId, "Task " + statuses[i], expectedCodes[i]);
            
            // 清理 gauge
            Map<String, ?> gauges = getPrivateFieldTaskGauges(inspectTaskStatusMetrics);
            gauges.clear();
            testRegistry.clear();
        }
    }

    @Test
    @DisplayName("Gauge 的 help text 应该包含所有状态映射")
    void getHelpText_shouldContainAllStatusMappings() {
        // 通过反射调用私有方法
        String helpText = invokeGetHelpText(inspectTaskStatusMetrics);
        
        assertThat(helpText).contains("PASSED: 1");
        assertThat(helpText).contains("ERROR: 2");
        assertThat(helpText).contains("RUNNING: 3");
        assertThat(helpText).contains("FAILED: 4");
        assertThat(helpText).contains("DONE: 5");
        assertThat(helpText).contains("WAITING: 6");
        assertThat(helpText).contains("SCHEDULING: 7");
        assertThat(helpText).contains("STOPPING: 8");
        assertThat(helpText).contains("other: 0");
    }

    @Test
    @DisplayName("应该处理包含特殊字符的任务名称")
    void refreshTaskStatuses_shouldHandleTaskNamesWithSpecialCharacters() {
        setPrivateFieldTaskService(inspectTaskStatusMetrics, baseService);
        
        String specialName = "Task with special chars: @#$%^&*()_+{}|:<>?";
        InspectEntity task = createInspectEntity("69902630d652589d497a8abb", "passed", specialName);
        when(baseService.findAllEntity(any(Query.class))).thenReturn(List.of(task));
        
        inspectTaskStatusMetrics.refreshTaskStatuses();
        
        assertGaugeValueAndTags("69902630d652589d497a8abb", specialName, 1);
    }

    // 辅助方法：创建模拟 InspectEntity
    private InspectEntity createInspectEntity(String hexId, String status, String name) {
        InspectEntity entity = new InspectEntity();
        entity.setId(new ObjectId(hexId));
        entity.setStatus(status);
        entity.setName(name);
        return entity;
    }

    // 辅助断言：检查某个 task_id 的 gauge 值和 tag
    private void assertGaugeValueAndTags(String taskId, String expectedName, int expectedCode) {
        var gauge = testRegistry.find("inspect_task_status")
                .tag("task_id", taskId)
                .gauge();

        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(expectedCode);
        assertThat(gauge.getId().getTags()).anyMatch(tag ->
                "task_name".equals(tag.getKey()) && expectedName.equals(tag.getValue()));
    }

    // 通过反射获取 private 的 taskGauges map（仅测试用）
    @SuppressWarnings("unchecked")
    private Map<String, ?> getPrivateFieldTaskGauges(InspectTaskStatusMetrics metrics) {
        try {
            var field = InspectTaskStatusMetrics.class.getDeclaredField("taskGauges");
            field.setAccessible(true);
            return (Map<String, ?>) field.get(metrics);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 通过反射设置 private 的 taskService 字段（仅测试用）
    private void setPrivateFieldTaskService(InspectTaskStatusMetrics metrics, BaseService service) {
        try {
            var field = InspectTaskStatusMetrics.class.getDeclaredField("taskService");
            field.setAccessible(true);
            field.set(metrics, service);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 通过反射调用私有方法 getHelpText（仅测试用）
    private String invokeGetHelpText(InspectTaskStatusMetrics metrics) {
        try {
            var method = InspectTaskStatusMetrics.class.getDeclaredMethod("getHelpText");
            method.setAccessible(true);
            return (String) method.invoke(metrics);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
