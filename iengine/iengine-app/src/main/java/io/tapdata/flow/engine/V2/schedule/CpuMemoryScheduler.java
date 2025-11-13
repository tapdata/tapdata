package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.aspect.CpuMemUsageAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.TaskAspectManager;
import io.tapdata.entity.Usage;
import io.tapdata.threadgroup.CpuMemoryCollector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/10/9 11:37 Create
 * @description
 */
@Component
@Slf4j
public class CpuMemoryScheduler {
    @Autowired
    private ClientMongoOperator clientMongoOperator;

    @Scheduled(cron = "0/10 * * * * ?")
    public void collectCpuUsage() {
        try {
            CpuMemoryCollector.cleanOnce();
        } catch (Exception e) {
            log.warn("Clean task empty reference object failed: {}", e.getMessage());
        }
        reportOnce(null);
    }

    public void reportOnce(List<String> taskIds) {
        Map<String, Usage> stringUsageMap = CpuMemoryCollector.collectOnce(taskIds);
        Map<String, Object> metricInfo = new HashMap<>();
        stringUsageMap.forEach((id, usage) -> {
            Map<String, Number> usageInfoMap = new HashMap<>();
            usageInfoMap.put("memoryUsage", usage.getHeapMemoryUsage());
            usageInfoMap.put("cpuUsage", usage.getCpuUsage());
            usageInfoMap.put("lastUpdateTime", System.currentTimeMillis());
            AspectTask aspectTask = TaskAspectManager.get(id);
            metricInfo.put(id, usageInfoMap);
            CpuMemUsageAspect aspect = new CpuMemUsageAspect(usage);
            Optional.ofNullable(aspectTask)
                    .ifPresent(context -> context.onObserveAspect(aspect));
        });
        if (!metricInfo.isEmpty()) {
            clientMongoOperator.postOne(metricInfo, "/Task/update-cpu-memory", Void.class);
        }
    }
}
