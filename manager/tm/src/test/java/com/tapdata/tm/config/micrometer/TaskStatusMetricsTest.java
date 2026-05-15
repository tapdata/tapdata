package com.tapdata.tm.config.micrometer;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
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

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2026/2/14 15:31
 */
@ExtendWith(MockitoExtension.class)
class TaskStatusMetricsTest {

    @Mock
    private TaskService taskService;

    @InjectMocks
    private TaskStatusMetrics taskStatusMetrics;

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

        // 强制刷新内部 map（虽然不是必须，但更干净）
        // 通过反射或直接 new 一个新实例，但这里我们直接用注入的实例
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
    @DisplayName("应该正确映射任务状态到数字编码")
    void mapStatusToCode_shouldReturnCorrectCode() {
        TaskStatusMetrics metrics = new TaskStatusMetrics(mock(TaskService.class));

        assertThat(metrics.mapStatusToCode(null)).isZero();
        assertThat(metrics.mapStatusToCode("edit")).isEqualTo(1);
        assertThat(metrics.mapStatusToCode("running")).isEqualTo(5);
        assertThat(metrics.mapStatusToCode("error")).isEqualTo(9);
        assertThat(metrics.mapStatusToCode("complete")).isEqualTo(10);
        assertThat(metrics.mapStatusToCode("deleting")).isEqualTo(20);
        assertThat(metrics.mapStatusToCode("unknown")).isZero();
    }

    @Test
    @DisplayName("当有新任务时应该创建 Gauge，已删除任务应该移除")
    void refreshTaskStatuses_shouldCreateAndRemoveGaugesCorrectly() {
        // 准备模拟数据
        Instant now = Instant.now();

        TaskEntity t1 = createTask("69902630d652589d497a8ab1", "running", false, "Task One");
        TaskEntity t2 = createTask("69902630d652589d497a8ab2", "error", false, "Task Two");
        TaskEntity t3Deleted = createTask("69902630d652589d497a8ab3", "delete_failed", true, "Deleted Task");

        // 模拟数据库返回：t1 和 t2 存在，t3 已删除
        when(taskService.findAllEntity(any(Query.class)))
                .thenReturn(List.of(t1, t2));

        // 第一次调用 → 创建两个 Gauge
        taskStatusMetrics.refreshTaskStatuses();

        assertThat(testRegistry.find("task_status").gauges()).hasSize(3);

        // 验证 tag 和值
        assertGaugeValueAndTags("69902630d652589d497a8ab1", "Task One", 5);
        assertGaugeValueAndTags("69902630d652589d497a8ab2", "Task Two", 9);

        // 第二次调用 → 模拟 t3 已经不存在，且新增 t4
        TaskEntity t4 = createTask("69902630d652589d497a8ab4", "paused", false, "Task Four");
        when(taskService.findAllEntity(any(Query.class)))
                .thenReturn(List.of(t1, t4));

        taskStatusMetrics.refreshTaskStatuses();

        assertThat(testRegistry.find("task_status").gauges()).hasSize(4);
        assertGaugeValueAndTags("69902630d652589d497a8ab1", "Task One", 5);
        assertGaugeValueAndTags("69902630d652589d497a8ab4", "Task Four", 8);

        // 验证内部 map 也只剩下两个
        assertThat(getPrivateFieldTaskGauges().keySet()).containsExactlyInAnyOrder("69902630d652589d497a8ab1", "69902630d652589d497a8ab4");
    }

    @Test
    @DisplayName("已删除任务调用 destroy()")
    void shouldCallDestroyWhenTaskRemoved() {
        TaskEntity t1 = createTask("69902630d652589d497a8abb", "running", false, "A");

        when(taskService.findAllEntity(any(Query.class)))
                .thenReturn(List.of(t1))   // 第一次存在
                .thenReturn(Collections.emptyList());  // 第二次删除

        // 第一次 → 创建
        taskStatusMetrics.refreshTaskStatuses();
        assertThat(testRegistry.find("task_status").gauges()).hasSize(1);

        // 第二次 → 移除
        taskStatusMetrics.refreshTaskStatuses();

        // 验证内部 map 已清空
        assertThat(getPrivateFieldTaskGauges()).isEmpty();
    }

    // 辅助方法：创建模拟 TaskEntity
    private TaskEntity createTask(String hexId, String status, boolean isDeleted, String name) {
        TaskEntity task = new TaskEntity();
        task.setId(new ObjectId(hexId));
        task.setStatus(status);
        task.set_deleted(isDeleted);
        task.setName(name);
        task.setDeleteName("Deleted_" + name);
        return task;
    }

    // 辅助断言：检查某个 task_id 的 gauge 值和 tag
    private void assertGaugeValueAndTags(String taskId, String expectedName, int expectedCode) {
        var gauge = testRegistry.find("task_status")
                .tag("task_id", taskId)
                .gauge();

        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(expectedCode);
        assertThat(gauge.getId().getTags()).anyMatch(tag ->
                "task_name".equals(tag.getKey()) && expectedName.equals(tag.getValue()));
    }

    // 通过反射获取 private 的 taskGauges map（仅测试用）
    @SuppressWarnings("unchecked")
    private Map<String, ?> getPrivateFieldTaskGauges() {
        try {
            var field = TaskStatusMetrics.class.getDeclaredField("taskGauges");
            field.setAccessible(true);
            return (Map<String, ?>) field.get(taskStatusMetrics);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
