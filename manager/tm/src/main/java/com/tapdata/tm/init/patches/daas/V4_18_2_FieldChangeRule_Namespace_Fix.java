package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@PatchAnnotation(appType = AppType.DAAS, version = "4.18-2")
public class V4_18_2_FieldChangeRule_Namespace_Fix extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V4_18_2_FieldChangeRule_Namespace_Fix.class);

    public V4_18_2_FieldChangeRule_Namespace_Fix(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        TaskService taskService = SpringContextHelper.getBean(TaskService.class);
        MetadataInstancesService metadataInstancesService = SpringContextHelper.getBean(MetadataInstancesService.class);
        TaskRepository taskRepository = SpringContextHelper.getBean(TaskRepository.class);

        Query query = new Query(Criteria.where("dag.nodes.fieldChangeRules").exists(true).ne(Collections.emptyList())
                .and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                .and(("is_deleted")).ne(true));
        query.fields().include("syncType", "dag");
        List<TaskDto> tasks = taskService.findAll(query);
        if (CollectionUtils.isEmpty(tasks)) {
            logger.info("V4_19_1: no tasks need fix");
            return;
        }

        Map<String, List<TaskDto>> tasksBySyncType = tasks.stream()
                .collect(Collectors.groupingBy(TaskDto::getSyncType));
        List<TaskDto> migrateTasks = tasksBySyncType.getOrDefault(TaskDto.SYNC_TYPE_MIGRATE, Collections.emptyList());
        List<TaskDto> syncTasks = tasksBySyncType.getOrDefault(TaskDto.SYNC_TYPE_SYNC, Collections.emptyList());

        Set<String> qualifiedNames = new HashSet<>();
        for (TaskDto task : migrateTasks) {
            forEachRule(task, rule -> {
                String[] ns = rule.getNamespace();
                if (ns != null && ns.length >= 2 && ns[1] != null) qualifiedNames.add(ns[1]);
            });
        }
        Map<String, String> qnToAncestors = batchResolveAncestors(metadataInstancesService, qualifiedNames);

        int updated = 0;
        updated += updateMigrateTasks(taskRepository, migrateTasks, qnToAncestors);
        updated += updateSyncTasks(taskRepository, syncTasks);
        logger.info("V4_19_1 done, scanned tasks={}, updated tasks={}, resolved qualifiedNames={}/{}",
                tasks.size(), updated, qnToAncestors.size(), qualifiedNames.size());
    }

    private static Map<String, String> batchResolveAncestors(MetadataInstancesService svc, Set<String> qns) {
        if (qns.isEmpty()) return Collections.emptyMap();
        Query q = new Query(Criteria.where("qualified_name").in(qns));
        q.fields().include("qualified_name", "ancestorsName");
        List<MetadataInstancesDto> list = svc.findAll(q);
        Map<String, String> map = new HashMap<>();
        for (MetadataInstancesDto m : list) {
            if (m.getQualifiedName() != null && m.getAncestorsName() != null) {
                map.putIfAbsent(m.getQualifiedName(), m.getAncestorsName());
            }
        }
        return map;
    }

    private static int updateMigrateTasks(TaskRepository taskRepository, List<TaskDto> tasks,
                                          Map<String, String> qnToAncestors) {
        int count = 0;
        for (TaskDto task : tasks) {
            boolean changed = applyRules(task, rule -> {
                String[] ns = rule.getNamespace();
                if (ns == null || ns.length < 2 || ns[1] == null) return false;
                String ancestors = qnToAncestors.get(ns[1]);
                if (StringUtils.isBlank(ancestors) || ancestors.equals(ns[1])) return false;
                ns[1] = ancestors;
                return true;
            });
            if (changed) {
                writeBack(taskRepository, task);
                count++;
            }
        }
        return count;
    }

    private static int updateSyncTasks(TaskRepository taskRepository, List<TaskDto> tasks) {
        int count = 0;
        for (TaskDto task : tasks) {
            boolean changed = applyRules(task, rule -> {
                String[] ns = rule.getNamespace();
                if (ns == null || ns.length < 2 || ns[1] == null) return false;
                ns[1] = null;
                return true;
            });
            if (changed) {
                writeBack(taskRepository, task);
                count++;
            }
        }
        return count;
    }

    private static void forEachRule(TaskDto task, java.util.function.Consumer<FieldChangeRule> consumer) {
        if (task.getDag() == null || task.getDag().getNodes() == null) return;
        for (Node<?> node : task.getDag().getNodes()) {
            if (!(node instanceof DataParentNode)) continue;
            List<FieldChangeRule> rules = ((DataParentNode<?>) node).getFieldChangeRules();
            if (CollectionUtils.isEmpty(rules)) continue;
            for (FieldChangeRule rule : rules) consumer.accept(rule);
        }
    }

    private static boolean applyRules(TaskDto task, java.util.function.Function<FieldChangeRule, Boolean> mutator) {
        AtomicBoolean changed = new AtomicBoolean(false);
        forEachRule(task, rule -> {
            if (Boolean.TRUE.equals(mutator.apply(rule))) changed.set(true);
        });
        return changed.get();
    }

    private static void writeBack(TaskRepository taskRepository, TaskDto task) {
        Query q = new Query(Criteria.where("_id").is(task.getId()));
        taskRepository.update(q, Update.update("dag", task.getDag()));
    }
}
