package io.tapdata.observable.logging.cache;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

class TaskCacheContext {
    private static final String SEGMENTS_DIR = "segments";
    private static final String TASK_NAME_FILE = "task-name";

    private enum State {
        ACTIVE,
        STOPPED,
        DELETED
    }

    private final String taskId;
    private final Path taskPath;
    private final CacheObserveLogConfig config;
    private final MonitoringLogCodec codec;
    private final CacheObserveLogAudit audit;
    private final LongSupplier totalCacheBytes;
    private final CacheLogDispatcher dispatcher;
    private final ReentrantLock lock = new ReentrantLock();
    private final NavigableMap<Long, CacheGeneration> generations = new TreeMap<>();

    private String taskName;
    private State state = State.STOPPED;

    TaskCacheContext(String taskId,
                     String taskName,
                     Path taskPath,
                     CacheObserveLogConfig config,
                     MonitoringLogCodec codec,
                     CacheObserveLogAudit audit,
                     LongSupplier totalCacheBytes,
                     CacheLogDispatcher dispatcher) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.taskPath = taskPath;
        this.config = config;
        this.codec = codec;
        this.audit = audit;
        this.totalCacheBytes = totalCacheBytes;
        this.dispatcher = dispatcher;
    }

    boolean activate(String name) {
        lock.lock();
        try {
            if (state == State.DELETED) {
                return false;
            }
            taskName = name;
            openInventory();
            enforceRetention();
            persistTaskName();
            state = State.ACTIVE;
            return true;
        } catch (IOException e) {
            throw new IllegalStateException("Activate task cache failed: " + taskId, e);
        } finally {
            lock.unlock();
        }
    }

    boolean append(MonitoringLogsDto log) {
        lock.lock();
        try {
            if (state != State.ACTIVE) {
                return false;
            }
            CacheGeneration active = activeGeneration();
            active.append(log);
            if (active.sizeBytes() >= config.getMaxFileSizeBytes()) {
                rotate(active.getId() + 1L);
            }
            return true;
        } catch (IOException e) {
            throw new IllegalStateException("Append task cache failed: " + taskId, e);
        } finally {
            lock.unlock();
        }
    }

    boolean poll(CacheLogSink sink) {
        lock.lock();
        try {
            if (state != State.ACTIVE) {
                return false;
            }
            for (CacheGeneration generation : generations.values()) {
                if (generation.poll(sink, dispatcher)) {
                    return true;
                }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    void stop() {
        lock.lock();
        try {
            if (state == State.DELETED) {
                return;
            }
            state = State.STOPPED;
            closeGenerations();
        } finally {
            lock.unlock();
        }
    }

    void delete() {
        lock.lock();
        try {
            state = State.DELETED;
            closeGenerations();
            try {
                CacheGeneration.deleteRecursively(taskPath);
            } catch (IOException e) {
                audit.deleteFailed(taskId, taskName, taskPath, e);
                throw new IllegalStateException("Delete task cache failed: " + taskId, e);
            }
        } finally {
            lock.unlock();
        }
    }

    private void openInventory() throws IOException {
        if (!generations.isEmpty()) {
            return;
        }
        Path segments = taskPath.resolve(SEGMENTS_DIR);
        Files.createDirectories(segments);
        try (Stream<Path> paths = Files.list(segments)) {
            paths.filter(Files::isDirectory)
                    .map(this::generationEntry)
                    .filter(entry -> entry != null)
                    .forEach(entry -> generations.put(entry.getKey(), entry.getValue()));
        }
        if (generations.isEmpty()) {
            createGeneration(0L);
        }
    }

    private Map.Entry<Long, CacheGeneration> generationEntry(Path path) {
        try {
            long id = Long.parseLong(path.getFileName().toString());
            return new java.util.AbstractMap.SimpleImmutableEntry<>(id, new CacheGeneration(id, path, codec));
        } catch (NumberFormatException e) {
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("Open cache generation failed: " + path, e);
        }
    }

    private CacheGeneration activeGeneration() throws IOException {
        if (generations.isEmpty()) {
            return createGeneration(0L);
        }
        return generations.lastEntry().getValue();
    }

    private CacheGeneration createGeneration(long id) throws IOException {
        Path path = taskPath.resolve(SEGMENTS_DIR).resolve(String.format("%020d", id));
        CacheGeneration generation = new CacheGeneration(id, path, codec);
        generations.put(id, generation);
        return generation;
    }

    private void rotate(long nextGenerationId) throws IOException {
        while (generations.size() > config.getMaxBackupIndex()) {
            evictOldest();
        }
        createGeneration(nextGenerationId);
    }

    private void enforceRetention() throws IOException {
        while (generations.size() - 1 > config.getMaxBackupIndex()) {
            evictOldest();
        }
    }

    private void evictOldest() throws IOException {
        Map.Entry<Long, CacheGeneration> oldest = generations.firstEntry();
        CacheGeneration generation = oldest.getValue();
        boolean dataLoss = !generation.isFullyConsumed();
        String deletedPath = generation.getPath().toString();
        try {
            generation.deleteClosed();
        } catch (IOException e) {
            audit.deleteFailed(taskId, taskName, generation.getPath(), e);
            throw e;
        }
        generations.remove(oldest.getKey());
        audit.evict(
                taskId,
                taskName,
                generation.getId(),
                dataLoss,
                deletedPath,
                CacheGeneration.directorySize(taskPath),
                totalCacheBytes.getAsLong());
    }

    private void closeGenerations() {
        for (CacheGeneration generation : generations.values()) {
            generation.close();
        }
        generations.clear();
    }

    private void persistTaskName() throws IOException {
        String value = taskName == null ? "" : taskName;
        Files.writeString(
                taskPath.resolve(TASK_NAME_FILE),
                value,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }
}
