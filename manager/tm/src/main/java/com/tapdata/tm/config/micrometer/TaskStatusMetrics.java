package com.tapdata.tm.config.micrometer;

import com.tapdata.tm.task.constant.TaskStatusEnum;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2026/2/14 12:23
 */
@Component
public class TaskStatusMetrics {

    private final Map<String, TaskStatusGauge> taskGauges = new HashMap<>();

    private TaskService taskService;

    public TaskStatusMetrics(TaskService taskService) {
        this.taskService = taskService;
    }

    @Scheduled(fixedRate = 5000, initialDelay = 10000)
    public void refreshTaskStatuses() {
        Criteria criteria = new Criteria().orOperator(
                Criteria.where("is_deleted").is(true)
                        .and("last_updated").gte(Instant.now().plus(-1, ChronoUnit.DAYS)),
                Criteria.where("is_deleted").is(false),
                Criteria.where("is_deleted").exists(false)
        );
        Query query = Query.query(criteria);
        query.fields().include("id", "name", "deleteName", "status");

        List<TaskEntity> tasks = taskService.findAllEntity(query);

        List<String> ids = new ArrayList<>();
        for (TaskEntity task : tasks) {
            String taskId = task.getId().toHexString();
            ids.add(taskId);
            // 不存在时添加任务状态指标；更新任务状态
            taskGauges.computeIfAbsent(taskId, id -> new TaskStatusGauge(task))
                    .setStatusCode(mapStatusToCode(task.getStatus()));
        }

        // 移除已经删除的任务
        Set<String> deletedIds = taskGauges.keySet().stream().filter(id -> !ids.contains(id)).collect(Collectors.toSet());
        deletedIds.forEach(id -> {
            TaskStatusGauge taskGauge = taskGauges.remove(id);
            taskGauge.destroy();
        });
    }

    int mapStatusToCode(String status) {
        if (status == null)
            return 0;
        return switch (status) {
            case "edit" -> 1;
            case "scheduling" -> 2;
            case "schedule_failed" -> 3;
            case "wait_run" -> 4;
            case "running" -> 5;
            case "stopping" -> 6;
            case "pausing" -> 7;
            case "paused" -> 8;
            case "error" -> 9;
            case "complete" -> 10;
            case "stop" -> 11;
            case "wait_start" -> 12;
            case "deleting", "delete_failed" -> 20;
            default -> 0;
        };
    }

    private String getHelpText() {
        return TaskStatusEnum.getAllStatus().stream().map(s -> String.format("%s: %s", s, mapStatusToCode(s)))
                .collect(Collectors.joining(", ")) + ", deleted: 20";
    }

    private class TaskStatusGauge {
        private Gauge gauge;
        @Getter
        @Setter
        private int statusCode;
        @Getter
        private String taskId;
        public TaskStatusGauge(TaskEntity task) {
            this.taskId = task.getId().toHexString();
            statusCode = mapStatusToCode(task.getStatus());

            String taskName = statusCode == 20 ? task.getDeleteName() : task.getName();

            this.gauge = Gauge.builder("task_status", () -> statusCode)
                    .tag("task_id", taskId)
                    .tag("task_name", taskName)
                    .description("Current status of task, status code map is: " + getHelpText() )
                    .register(Metrics.globalRegistry);
        }
        public void destroy() {
            gauge.close();
            gauge = null;
        }
    }
}
