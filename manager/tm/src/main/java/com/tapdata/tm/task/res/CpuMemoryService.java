package com.tapdata.tm.task.res;

import com.tapdata.tm.monitor.entity.MeasurementEntity;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/10/13 11:53 Create
 * @description
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class CpuMemoryService {
    MongoTemplate mongoOperations;

    public Map<String, Map<String, Object>> cpuMemoryUsageOfTask(List<String> taskIds) {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("tags.taskId").in(taskIds)
                        .and("grnty").is("minute")
                        .and("tags.type").is("task")),
                Aggregation.sort(Sort.Direction.DESC, "date"),
                Aggregation.group("tags.taskId")
                        .first("$$ROOT").as("firstRecord")
        );
        List<Map> mappedResults = mongoOperations.aggregate(aggregation, MeasurementEntity.COLLECTION_NAME, Map.class)
                .getMappedResults();
        Map<String, Map<String, Object>> info = new HashMap<>();
        mappedResults.forEach(item -> {
            if (item.get("firstRecord") instanceof Map map
                    && map.get("tags") instanceof Map tags
                    && null != tags.get("taskId")) {
                Map<String, Object> metricInfo = info.computeIfAbsent(String.valueOf(tags.get("taskId")), k -> new HashMap<>());
                if (map.get("ss") instanceof Collection<?> samples) {
                    samples.stream()
                            .filter(Objects::nonNull)
                            .filter(s -> Objects.nonNull(((Map) s).get("date")))
                            .max(Comparator.comparing(s -> (Date) ((Map) s).get("date")))
                            .ifPresent(s -> {
                                metricInfo.put("lastUpdateTime", ((Map) s).get("date"));
                                if (s instanceof Map sInfo
                                        && sInfo.get("vs") instanceof Map sInfoVs) {
                                    Optional.ofNullable(sInfoVs.get("cpuUsage")).ifPresent(e -> metricInfo.put("cpuUsage", e));
                                    Optional.ofNullable(sInfoVs.get("memoryUsage")).ifPresent(e -> metricInfo.put("memoryUsage", e));
                                }
                            });
                }
            }
        });
        return info;
    }
}
