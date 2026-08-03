package io.tapdata.observable.logging.cache;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class CacheGeneration implements AutoCloseable {
    static final String FILE_TAILER_ID = "FILE_APPENDER_TAILER";
    static final String TM_TAILER_ID = "TM_APPENDER_TAILER";
    static final String PAYLOAD_BYTES_FILE = "payload-bytes";
    private static final String DISPATCH_FAILED_FILE = "dispatch-failed";
    // Chronicle reserves an additional overlap region, so a 3MB block keeps a new queue file within 4MB.
    private static final long CHRONICLE_BLOCK_SIZE_BYTES = 3L * 1024L * 1024L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheGeneration.class);

    private final long id;
    private final Path path;
    private final MonitoringLogCodec codec;
    private ChronicleQueue queue;
    private ExcerptTailer fileTailer;
    private ExcerptTailer tmTailer;
    private FileChannel payloadBytesChannel;
    private long sizeBytes;
    private int inFlightDispatches;
    private boolean dispatchFailed;
    private boolean payloadBytesInitialized;
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

    void append(MonitoringLogsDto log) throws IOException {
        ensureOpen();
        initializePayloadBytes();
        long[] appendedBytes = new long[1];
        try (ExcerptAppender appender = queue.acquireAppender()) {
            appender.writeDocument(wire ->
                    appendedBytes[0] = codec.writeAndMeasure(wire.getValueOut(), log));
        }
        long updatedSize = Math.addExact(sizeBytes, appendedBytes[0]);
        persistPayloadBytes(updatedSize);
        sizeBytes = updatedSize;
    }

    MonitoringLogsDto poll(CacheLogSink sink) {
        if (!open) {
            return null;
        }
        ExcerptTailer tailer = sink == CacheLogSink.FILE ? fileTailer : tmTailer;
        AtomicReference<MonitoringLogsDto> decodedLog = new AtomicReference<>();
        boolean read = tailer.readDocument(wire -> decodedLog.set(codec.read(wire.getValueIn())));
        if (read) {
            inFlightDispatches++;
        }
        return read ? decodedLog.get() : null;
    }

    void completeDispatch(boolean succeeded) {
        if (inFlightDispatches <= 0) {
            throw new IllegalStateException("No cached log dispatch is in flight: " + path);
        }
        inFlightDispatches--;
        if (!succeeded) {
            dispatchFailed = true;
            try {
                Files.write(
                        path.resolve(DISPATCH_FAILED_FILE),
                        new byte[0],
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE);
            } catch (IOException e) {
                LOGGER.warn("Persist CacheObserveLogs dispatch failure failed, path={}", path, e);
            }
        }
    }

    boolean isFullyConsumed() {
        if (!open) {
            return false;
        }
        long lastIndex = queue.lastIndex();
        return !dispatchFailed
                && inFlightDispatches == 0
                && (lastIndex == Long.MIN_VALUE
                || (hasConsumedThrough(fileTailer, lastIndex)
                && hasConsumedThrough(tmTailer, lastIndex)));
    }

    long sizeBytes() {
        return sizeBytes;
    }

    void initializePayloadBytes() throws IOException {
        if (payloadBytesInitialized) {
            return;
        }
        sizeBytes = recoverPayloadBytes();
        persistPayloadBytes(sizeBytes);
        payloadBytesInitialized = true;
    }

    void seal() {
        closePayloadBytesChannel();
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
        closePayloadBytesChannel();
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
        queue = ChronicleQueue.singleBuilder(path)
                .blockSize(CHRONICLE_BLOCK_SIZE_BYTES)
                .build();
        fileTailer = queue.createTailer(FILE_TAILER_ID).disableThreadSafetyCheck(true);
        tmTailer = queue.createTailer(TM_TAILER_ID).disableThreadSafetyCheck(true);
        Path payloadBytesPath = path.resolve(PAYLOAD_BYTES_FILE);
        OptionalLong persistedPayloadBytes = readPayloadBytes(payloadBytesPath);
        if (persistedPayloadBytes.isPresent()) {
            sizeBytes = persistedPayloadBytes.getAsLong();
            payloadBytesInitialized = true;
        }
        dispatchFailed = Files.isRegularFile(path.resolve(DISPATCH_FAILED_FILE));
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

    private long recoverPayloadBytes() {
        long recoveredBytes = 0L;
        try (ExcerptTailer recoveryTailer = queue.createTailer().toStart()) {
            AtomicReference<MonitoringLogsDto> decodedLog = new AtomicReference<>();
            while (recoveryTailer.readDocument(wire -> decodedLog.set(codec.read(wire.getValueIn())))) {
                recoveredBytes = Math.addExact(recoveredBytes, codec.measure(decodedLog.get()));
            }
        }
        return recoveredBytes;
    }

    private OptionalLong readPayloadBytes(Path payloadBytesPath) throws IOException {
        if (!Files.isRegularFile(payloadBytesPath)) {
            return OptionalLong.empty();
        }
        try (FileChannel channel = FileChannel.open(payloadBytesPath, StandardOpenOption.READ)) {
            if (channel.size() != Long.BYTES) {
                return OptionalLong.empty();
            }
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer);
                if (read <= 0) {
                    return OptionalLong.empty();
                }
            }
            buffer.flip();
            long payloadBytes = buffer.getLong();
            return payloadBytes >= 0L ? OptionalLong.of(payloadBytes) : OptionalLong.empty();
        }
    }

    private void persistPayloadBytes(long payloadBytes) throws IOException {
        if (payloadBytesChannel == null) {
            payloadBytesChannel = FileChannel.open(
                    path.resolve(PAYLOAD_BYTES_FILE),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(payloadBytes);
        buffer.flip();
        payloadBytesChannel.position(0L);
        while (buffer.hasRemaining()) {
            int written = payloadBytesChannel.write(buffer);
            if (written <= 0) {
                throw new IOException("Persist payload counter made no progress: " + path);
            }
        }
        payloadBytesChannel.truncate(Long.BYTES);
    }

    private void closePayloadBytesChannel() {
        if (payloadBytesChannel == null) {
            return;
        }
        try {
            payloadBytesChannel.force(true);
        } catch (IOException e) {
            LOGGER.warn("Flush CacheObserveLogs payload counter failed, path={}", path, e);
        } finally {
            try {
                payloadBytesChannel.close();
            } catch (IOException e) {
                LOGGER.warn("Close CacheObserveLogs payload counter failed, path={}", path, e);
            }
            payloadBytesChannel = null;
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
