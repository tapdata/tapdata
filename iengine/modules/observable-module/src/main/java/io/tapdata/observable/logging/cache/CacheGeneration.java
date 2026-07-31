package io.tapdata.observable.logging.cache;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class CacheGeneration implements AutoCloseable {
    static final String FILE_TAILER_ID = "FILE_APPENDER_TAILER";
    static final String TM_TAILER_ID = "TM_APPENDER_TAILER";

    private final long id;
    private final Path path;
    private final MonitoringLogCodec codec;
    private ChronicleQueue queue;
    private ExcerptTailer fileTailer;
    private ExcerptTailer tmTailer;
    private long sizeBytes;
    private boolean open;

    CacheGeneration(long id, Path path, MonitoringLogCodec codec) throws IOException {
        this.id = id;
        this.path = path;
        this.codec = codec;
        open();
    }

    long getId() {
        return id;
    }

    Path getPath() {
        return path;
    }

    void append(MonitoringLogsDto log) {
        ensureOpen();
        long[] appendedBytes = new long[1];
        try (ExcerptAppender appender = queue.acquireAppender()) {
            appender.writeDocument(wire ->
                    appendedBytes[0] = codec.writeAndMeasure(wire.getValueOut(), log));
        }
        sizeBytes += appendedBytes[0];
    }

    MonitoringLogsDto poll(CacheLogSink sink) {
        if (!open) {
            return null;
        }
        ExcerptTailer tailer = sink == CacheLogSink.FILE ? fileTailer : tmTailer;
        AtomicReference<MonitoringLogsDto> record = new AtomicReference<>();
        boolean read = tailer.readDocument(wire -> record.set(codec.read(wire.getValueIn())));
        return read ? record.get() : null;
    }

    boolean isFullyConsumed() {
        if (!open) {
            return false;
        }
        long lastIndex = queue.lastIndex();
        return lastIndex == Long.MIN_VALUE
                || (hasConsumedThrough(fileTailer, lastIndex)
                && hasConsumedThrough(tmTailer, lastIndex));
    }

    long sizeBytes() {
        return sizeBytes;
    }

    void deleteClosed() throws IOException {
        close();
        deleteRecursively(path);
    }

    @Override
    public void close() {
        if (!open) {
            return;
        }
        open = false;
        if (fileTailer != null) {
            fileTailer.close();
            fileTailer = null;
        }
        if (tmTailer != null) {
            tmTailer.close();
            tmTailer = null;
        }
        if (queue != null) {
            queue.close();
            queue = null;
        }
    }

    static long directorySize(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return 0L;
        }
        try (Stream<Path> files = Files.walk(directory)) {
            try {
                return files.filter(Files::isRegularFile)
                        .mapToLong(CacheGeneration::fileSize)
                        .sum();
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }
    }

    static void deleteRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            Path[] ordered = paths.sorted(Comparator.reverseOrder()).toArray(Path[]::new);
            for (Path path : ordered) {
                Files.deleteIfExists(path);
            }
        }
    }

    private void open() throws IOException {
        Files.createDirectories(path);
        queue = ChronicleQueue.singleBuilder(path).build();
        fileTailer = queue.createTailer(FILE_TAILER_ID).disableThreadSafetyCheck(true);
        tmTailer = queue.createTailer(TM_TAILER_ID).disableThreadSafetyCheck(true);
        sizeBytes = directorySize(path);
        open = true;
    }

    private void ensureOpen() {
        if (!open) {
            throw new IllegalStateException("Cache generation is closed: " + path);
        }
    }

    private boolean hasConsumedThrough(ExcerptTailer tailer, long lastIndex) {
        // A named tailer's index is its persisted next-read position. lastReadIndex is
        // process-local and resets to zero when the tailer is reopened.
        return tailer.index() > lastIndex;
    }

    private static long fileSize(Path file) {
        try {
            return Files.size(file);
        } catch (NoSuchFileException e) {
            return 0L;
        } catch (IOException e) {
            throw new UncheckedIOException("Read cache file size failed: " + file, e);
        }
    }
}
