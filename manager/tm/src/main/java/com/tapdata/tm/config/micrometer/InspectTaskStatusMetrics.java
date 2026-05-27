package com.tapdata.tm.config.micrometer;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.inspect.service.InspectTaskService;
import com.tapdata.tm.inspect.service.InspectTaskServiceOOSImpl;
import com.tapdata.tm.task.constant.TaskStatusEnum;
import com.tapdata.tm.task.entity.TaskEntity;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2026/2/14 12:23
 */
@Component
public class InspectTaskStatusMetrics {

    private final Map<String, InspectTaskStatusGauge> taskGauges = new HashMap<>();

    private BaseService taskService;

    public InspectTaskStatusMetrics(InspectTaskService taskService) {
        if (taskService instanceof BaseService<?,?,?,?> baseService)
            this.taskService = baseService;
    }

    @Scheduled(fixedRate = 5000, initialDelay = 10000)
    public void refreshTaskStatuses() {
        if (this.taskService == null) return;

        Criteria criteria = Criteria.where("is_deleted").ne(true);
        Query query = Query.query(criteria);
        query.fields().include("id", "name", "mode", "inspectMethod", "status", "result");

        List<InspectEntity> tasks = taskService.findAllEntity(query);
        if (tasks == null) {
            return;
        }

        List<String> ids = new ArrayList<>();
        for (InspectEntity task : tasks) {
            if (task.getId() == null || StringUtils.isBlank(task.getStatus())) {
                continue;
            }
            String taskId = task.getId().toHexString();
            ids.add(taskId);
            // 不存在时添加任务状态指标；更新任务状态
            taskGauges.computeIfAbsent(taskId, id -> new InspectTaskStatusGauge(task))
                    .setStatusCode(mapStatusToCode(task.getStatus()));
        }

        // 移除已经删除的任务
        Set<String> deletedIds = taskGauges.keySet().stream().filter(id -> !ids.contains(id)).collect(Collectors.toSet());
        deletedIds.forEach(id -> {
            InspectTaskStatusGauge taskGauge = taskGauges.remove(id);
            taskGauge.destroy();
        });
    }

    int mapStatusToCode(String status) {
        if (status == null)
            return 0;
        InspectStatusEnum statusEnum = InspectStatusEnum.of(status);
        if (statusEnum == null)
            return 0;
        return switch (statusEnum) {
            case PASSED -> 1;
            case ERROR -> 2;
            case RUNNING -> 3;
            case FAILED -> 4;
            case DONE -> 5;
            case WAITING -> 6;
            case SCHEDULING -> 7;
            case STOPPING -> 8;
            default -> 0;
        };
    }

    private String getHelpText() {
        return Stream.of(InspectStatusEnum.values()).map(s -> String.format("%s: %s", s, mapStatusToCode(s.getValue())))
                .collect(Collectors.joining(", ")) + ", other: 0";
    }

    private class InspectTaskStatusGauge {
        private Gauge gauge;
        @Getter
        @Setter
        private int statusCode;
        @Getter
        private String taskId;
        public InspectTaskStatusGauge(InspectEntity task) {
            this.taskId = task.getId().toHexString();
            statusCode = mapStatusToCode(task.getStatus());

            String taskName = task.getName();

            this.gauge = Gauge.builder("inspect_task_status", () -> statusCode)
                    .tag("task_id", taskId)
                    .tag("task_name", taskName)
                    .description("Current status of inspect task, status code map is: " + getHelpText() )
                    .register(Metrics.globalRegistry);
        }
        public void destroy() {
            gauge.close();
            gauge = null;
        }
    }
}
