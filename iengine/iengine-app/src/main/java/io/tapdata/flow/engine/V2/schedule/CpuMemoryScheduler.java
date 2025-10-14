package io.tapdata.flow.engine.V2.schedule;

import io.tapdata.aspect.CpuMemUsageAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.TaskAspectManager;
import io.tapdata.entity.Usage;
import io.tapdata.threadgroup.CpuMemoryCollector;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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
public class CpuMemoryScheduler {

    @Scheduled(cron = "0/5 * * * * ?")
    public void collectCpuUsage() {
        reportOnce(null);
    }

    public void reportOnce(List<String> taskIds) {
        Map<String, Usage> stringUsageMap = CpuMemoryCollector.collectOnce(taskIds);
        stringUsageMap.forEach((id, usage) -> {
            AspectTask aspectTask = TaskAspectManager.get(id);
            CpuMemUsageAspect aspect = new CpuMemUsageAspect(usage);
            Optional.ofNullable(aspectTask)
                    .ifPresent(context -> context.onObserveAspect(aspect));
        });
    }
}
