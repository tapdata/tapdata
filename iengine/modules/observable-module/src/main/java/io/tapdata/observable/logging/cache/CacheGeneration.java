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
        try (ExcerptAppender appender = queue.acquireAppender()) {
            appender.writeDocument(wire -> codec.write(wire.getValueOut(), log));
        }
    }

    boolean poll(CacheLogSink sink, CacheLogDispatcher dispatcher) {
        if (!open) {
            return false;
        }
        ExcerptTailer tailer = sink == CacheLogSink.FILE ? fileTailer : tmTailer;
        AtomicReference<MonitoringLogsDto> record = new AtomicReference<>();
        boolean read = tailer.readDocument(wire -> record.set(codec.read(wire.getValueIn())));
        if (read) {
            dispatcher.dispatch(record.get(), sink);
        }
        return read;
    }

    boolean isFullyConsumed() {
        if (!open) {
            return false;
        }
        long lastIndex = queue.lastIndex();
        return lastIndex == Long.MIN_VALUE
                || (fileTailer.lastReadIndex() >= lastIndex && tmTailer.lastReadIndex() >= lastIndex);
    }

    long sizeBytes() throws IOException {
        return directorySize(path);
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
        open = true;
    }

    private void ensureOpen() {
        if (!open) {
            throw new IllegalStateException("Cache generation is closed: " + path);
        }
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
