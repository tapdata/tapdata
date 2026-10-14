package com.tapdata.tm.fieldorigin.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.fieldorigin.builder.FieldOriginBuildResult;
import com.tapdata.tm.fieldorigin.builder.FieldOriginBuilder;
import com.tapdata.tm.fieldorigin.builder.FieldOriginRecord;
import com.tapdata.tm.fieldorigin.dto.FieldOriginDto;
import com.tapdata.tm.fieldorigin.dto.FieldOriginQueryParam;
import com.tapdata.tm.fieldorigin.dto.FieldOriginQueryResult;
import com.tapdata.tm.fieldorigin.dto.FieldOriginSnapshot;
import com.tapdata.tm.fieldorigin.dto.FieldOriginVersionDto;
import com.tapdata.tm.fieldorigin.dto.FieldOriginVersionInfo;
import com.tapdata.tm.fieldorigin.entity.FieldOriginEntity;
import com.tapdata.tm.fieldorigin.entity.FieldOriginVersionEntity;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Builds, versions and queries field origins of task target tables (TAP-13082).
 * <p>
 * A new version is written completely before it becomes active, so readers never see a partial version;
 * a failed build keeps the previous active version. Builds with an unchanged config hash do not create versions.
 */
@Slf4j
@Service
public class FieldOriginService {
    public static final String TRIGGER_MANUAL = "manual";
    public static final String TRIGGER_TRANSFORM = "transform";
    private static final String[] TASK_FIELDS = {"_id", "dag", "customId", "user_id"};
    private static final int MAX_ERROR_LENGTH = 2000;

    private final FieldOriginStore store;
    private final FieldOriginTaskLoader loader;
    private final Map<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final Set<String> pending = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor;

    @Autowired
    @Lazy
    private TaskService taskService;

    @Value("${tm.fieldOrigin.keepVersions:5}")
    private int keepVersions = 5;

    @Value("${tm.fieldOrigin.autoBuild:true}")
    private boolean autoBuild = true;

    public FieldOriginService(FieldOriginStore store, FieldOriginTaskLoader loader) {
        this.store = store;
        this.loader = loader;
        AtomicInteger index = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(1, 2, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000),
                r -> {
                    Thread thread = new Thread(r, "field-origin-builder-" + index.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.DiscardPolicy());
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }

    /**
     * Rebuilds asynchronously after the task models are derived, requests of a task waiting in the queue are merged.
     * Never throws so schema transform is not affected.
     */
    public void rebuildAsync(String taskId) {
        if (!autoBuild || StringUtils.isBlank(taskId) || !pending.add(taskId)) {
            return;
        }
        try {
            executor.execute(() -> {
                pending.remove(taskId);
                try {
                    TaskDto task = taskService.findByTaskId(MongoUtils.toObjectId(taskId), TASK_FIELDS);
                    if (null != task) {
                        rebuild(task, TRIGGER_TRANSFORM);
                    }
                } catch (Exception e) {
                    log.warn("Rebuild field origin of task {} failed: {}", taskId, e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            pending.remove(taskId);
            log.warn("Submit field origin rebuild of task {} failed: {}", taskId, e.getMessage());
        }
    }

    public FieldOriginVersionDto rebuild(String taskId, UserDetail user) {
        return rebuild(checkTask(taskId, user, TASK_FIELDS), TRIGGER_MANUAL);
    }

    public FieldOriginVersionDto rebuild(TaskDto task, String trigger) {
        String taskId = task.getId().toHexString();
        ReentrantLock lock = locks.computeIfAbsent(taskId, k -> new ReentrantLock());
        lock.lock();
        try {
            return doRebuild(task, taskId, trigger);
        } finally {
            lock.unlock();
        }
    }

    FieldOriginVersionDto doRebuild(TaskDto task, String taskId, String trigger) {
        Date startAt = new Date();
        FieldOriginBuildResult result;
        try {
            result = new FieldOriginBuilder(loader.graph(task), loader.schemaProvider(taskId)).build();
        } catch (Exception e) {
            log.warn("Build field origin of task {} failed: {}", taskId, e.getMessage(), e);
            FieldOriginVersionEntity failed = newVersion(task, taskId, store.nextVersion(taskId), trigger, startAt);
            fail(failed, e);
            return toDto(failed);
        }
        FieldOriginVersionEntity active = store.findActive(taskId);
        if (null != active && Objects.equals(active.getConfigHash(), result.getConfigHash())) {
            return toDto(active);
        }
        FieldOriginVersionEntity version = newVersion(task, taskId, store.nextVersion(taskId), trigger, startAt);
        version.setConfigHash(result.getConfigHash());
        version.setStats(result.getStats());
        store.saveVersion(version);
        try {
            store.insertRecords(toEntities(task, taskId, version.getVersion(), result.getRecords()));
            version.setStatus(FieldOriginVersionEntity.STATUS_SUCCESS);
            version.setBuildEndAt(new Date());
            store.saveVersion(version);
            store.activate(taskId, version.getVersion());
            version.setActive(true);
        } catch (Exception e) {
            log.warn("Save field origin of task {} version {} failed: {}", taskId, version.getVersion(), e.getMessage(), e);
            try {
                store.deleteRecords(taskId, version.getVersion());
            } finally {
                fail(version, e);
            }
            return toDto(version);
        }
        prune(taskId);
        return toDto(version);
    }

    void prune(String taskId) {
        try {
            for (FieldOriginVersionEntity old : store.findPrunable(taskId, keepVersions)) {
                store.deleteVersion(taskId, old.getVersion());
            }
        } catch (Exception e) {
            log.warn("Prune field origin versions of task {} failed: {}", taskId, e.getMessage());
        }
    }

    void fail(FieldOriginVersionEntity version, Exception e) {
        version.setStatus(FieldOriginVersionEntity.STATUS_FAILED);
        version.setActive(false);
        version.setError(StringUtils.abbreviate(StringUtils.defaultIfBlank(e.getMessage(), e.getClass().getName()), MAX_ERROR_LENGTH));
        version.setBuildEndAt(new Date());
        store.saveVersion(version);
    }

    FieldOriginVersionEntity newVersion(TaskDto task, String taskId, long number, String trigger, Date startAt) {
        FieldOriginVersionEntity version = new FieldOriginVersionEntity();
        version.setTaskId(taskId);
        version.setVersion(number);
        version.setStatus(FieldOriginVersionEntity.STATUS_BUILDING);
        version.setActive(false);
        version.setTrigger(trigger);
        version.setBuildStartAt(startAt);
        version.setCustomId(task.getCustomId());
        version.setUserId(task.getUserId());
        version.setCreateAt(startAt);
        version.setLastUpdAt(startAt);
        return version;
    }

    List<FieldOriginEntity> toEntities(TaskDto task, String taskId, long version, List<FieldOriginRecord> records) {
        Date now = new Date();
        List<FieldOriginEntity> entities = new ArrayList<>(records.size());
        for (FieldOriginRecord r : records) {
            FieldOriginEntity e = new FieldOriginEntity();
            e.setTaskId(taskId);
            e.setVersion(version);
            e.setTargetNodeId(r.getTargetNodeId());
            e.setTargetConnectionId(r.getTargetConnectionId());
            e.setTargetTable(r.getTargetTable());
            e.setFieldPath(r.getFieldPath());
            e.setNormalizedFieldPath(r.getNormalizedFieldPath());
            e.setSourceNodeId(r.getSourceNodeId());
            e.setSourceConnectionId(r.getSourceConnectionId());
            e.setSourceTable(r.getSourceTable());
            e.setSourceField(r.getSourceField());
            e.setOriginType(null == r.getOriginType() ? null : r.getOriginType().name());
            e.setMatchMethod(null == r.getMatchMethod() ? null : r.getMatchMethod().name());
            e.setTransformNodeId(r.getTransformNodeId());
            e.setMergeNodeId(r.getMergeNodeId());
            e.setMergePropertyId(r.getMergePropertyId());
            e.setTargetPath(r.getTargetPath());
            e.setCustomId(task.getCustomId());
            e.setUserId(task.getUserId());
            e.setCreateAt(now);
            e.setLastUpdAt(now);
            entities.add(e);
        }
        return entities;
    }

    public FieldOriginQueryResult query(FieldOriginQueryParam param, UserDetail user) {
        if (null == param || CollectionUtils.isEmpty(param.getFieldPaths())) {
            throw new BizException("IllegalArgument", "fieldPaths");
        }
        checkTask(param.getTaskId(), user, "_id");
        return query(param);
    }

    FieldOriginQueryResult query(FieldOriginQueryParam param) {
        FieldOriginVersionEntity version = resolveVersion(param.getTaskId(), param.getVersion());
        FieldOriginQueryResult result = new FieldOriginQueryResult();
        result.setTaskId(param.getTaskId());
        Map<String, String> normalizedByRequest = new LinkedHashMap<>();
        param.getFieldPaths().stream().filter(StringUtils::isNotBlank)
                .forEach(p -> normalizedByRequest.put(p, FieldOriginBuilder.normalizeQueryPath(p)));
        if (null == version) {
            result.getNotFound().addAll(normalizedByRequest.keySet());
            return result;
        }
        result.setVersion(version.getVersion());
        result.setConfigHash(version.getConfigHash());
        List<FieldOriginEntity> records = store.findRecords(param.getTaskId(), version.getVersion(),
                param.getTargetConnectionId(), param.getTargetTable(), new LinkedHashSet<>(normalizedByRequest.values()));
        normalizedByRequest.forEach((request, path) -> {
            List<FieldOriginDto> matched = records.stream()
                    .filter(r -> matches(r.getNormalizedFieldPath(), path))
                    .map(this::toDto)
                    .toList();
            if (matched.isEmpty()) {
                result.getNotFound().add(request);
            } else {
                result.getOrigins().put(request, matched);
            }
        });
        return result;
    }

    static boolean matches(String recordPath, String path) {
        return null != recordPath && (recordPath.equals(path) || recordPath.startsWith(path + ".")
                || recordPath.startsWith(path + FieldOriginBuilder.ARRAY_SUFFIX_DOT));
    }

    public FieldOriginSnapshot snapshot(String taskId, Long version, String targetConnectionId, String targetTable, UserDetail user) {
        checkTask(taskId, user, "_id");
        return snapshot(taskId, version, targetConnectionId, targetTable);
    }

    FieldOriginSnapshot snapshot(String taskId, Long version, String targetConnectionId, String targetTable) {
        FieldOriginSnapshot snapshot = new FieldOriginSnapshot();
        FieldOriginVersionEntity resolved = resolveVersion(taskId, version);
        if (null == resolved) {
            return snapshot;
        }
        snapshot.setVersion(toDto(resolved));
        store.findRecords(taskId, resolved.getVersion(), targetConnectionId, targetTable, null)
                .forEach(r -> snapshot.getOrigins().add(toDto(r)));
        return snapshot;
    }

    public FieldOriginVersionInfo version(String taskId, UserDetail user) {
        checkTask(taskId, user, "_id");
        return version(taskId);
    }

    FieldOriginVersionInfo version(String taskId) {
        FieldOriginVersionInfo info = new FieldOriginVersionInfo();
        info.setActive(toDto(store.findActive(taskId)));
        info.setLatestBuild(toDto(store.findLatestBuild(taskId)));
        return info;
    }

    FieldOriginVersionEntity resolveVersion(String taskId, Long version) {
        if (null == version) {
            return store.findActive(taskId);
        }
        FieldOriginVersionEntity found = store.findVersion(taskId, version);
        return null != found && FieldOriginVersionEntity.STATUS_SUCCESS.equals(found.getStatus()) ? found : null;
    }

    TaskDto checkTask(String taskId, UserDetail user, String... fields) {
        ObjectId id = MongoUtils.toObjectId(taskId);
        if (null == id) {
            throw new BizException("IllegalArgument", "taskId");
        }
        return taskService.checkExistById(id, user, fields);
    }

    FieldOriginVersionDto toDto(FieldOriginVersionEntity entity) {
        if (null == entity) {
            return null;
        }
        FieldOriginVersionDto dto = new FieldOriginVersionDto();
        dto.setTaskId(entity.getTaskId());
        dto.setVersion(entity.getVersion());
        dto.setConfigHash(entity.getConfigHash());
        dto.setStatus(entity.getStatus());
        dto.setActive(entity.getActive());
        dto.setStats(entity.getStats());
        dto.setError(entity.getError());
        dto.setTrigger(entity.getTrigger());
        dto.setBuildStartAt(entity.getBuildStartAt());
        dto.setBuildEndAt(entity.getBuildEndAt());
        return dto;
    }

    FieldOriginDto toDto(FieldOriginEntity entity) {
        FieldOriginDto dto = new FieldOriginDto();
        dto.setTaskId(entity.getTaskId());
        dto.setVersion(entity.getVersion());
        dto.setTargetNodeId(entity.getTargetNodeId());
        dto.setTargetConnectionId(entity.getTargetConnectionId());
        dto.setTargetTable(entity.getTargetTable());
        dto.setFieldPath(entity.getFieldPath());
        dto.setNormalizedFieldPath(entity.getNormalizedFieldPath());
        dto.setSourceNodeId(entity.getSourceNodeId());
        dto.setSourceConnectionId(entity.getSourceConnectionId());
        dto.setSourceTable(entity.getSourceTable());
        dto.setSourceField(entity.getSourceField());
        dto.setOriginType(entity.getOriginType());
        dto.setMatchMethod(entity.getMatchMethod());
        dto.setTransformNodeId(entity.getTransformNodeId());
        dto.setMergeNodeId(entity.getMergeNodeId());
        dto.setMergePropertyId(entity.getMergePropertyId());
        dto.setTargetPath(entity.getTargetPath());
        return dto;
    }
}
