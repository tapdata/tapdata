package io.tapdata.pdk.core.memory;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CommandWorker implements Runnable {
    private static final String TAG = CommandWorker.class.getSimpleName();
    private Command command;
    private AtomicBoolean isClosed = new AtomicBoolean(false);
    private ConcurrentHashMap<String, MemoryFetcher> keyMemoryFetcherMap;
    private List<ScheduledFuture<?>> scheduledFutureList = new CopyOnWriteArrayList<>();

    public CommandWorker(Command command, ConcurrentHashMap<String, MemoryFetcher> keyMemoryFetcherMap) {
        this.command = command;
        this.keyMemoryFetcherMap = keyMemoryFetcherMap;
    }


    @Override
    public void run() {
        List<Execution> executionList = command.getExecutionList();
        if(executionList == null || executionList.isEmpty()) {
            TapLogger.warn(TAG, "No execution to do");
            return;
        }

        for(final Execution execution : executionList) {
            Runnable executionRunnable = () -> {
                List<String> customScopes = execution.getCustomScopes();
                Map<String, MemoryFetcher> finalMap = new HashMap<>();
                if(customScopes != null && !customScopes.isEmpty()) {
                    for(String customScope : customScopes) {
                        MemoryFetcher fetcher = keyMemoryFetcherMap.get(customScope);
                        if(fetcher != null) {
                            finalMap.put(customScope, fetcher);
                        }
                    }
                } else {
                    finalMap.putAll(keyMemoryFetcherMap);
                }
                CommandWorker.this.output(finalMap, execution);
            };
            Integer intervalSeconds = execution.getExpectIntervalSeconds();
            if(intervalSeconds != null && intervalSeconds > 0) {
                scheduledFutureList.add(ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(executionRunnable, 1, intervalSeconds, TimeUnit.SECONDS));
            } else {
                executionRunnable.run();
            }
        }
    }

    private void output(Map<String, MemoryFetcher> finalMap, Execution execution) {
        String outputType = execution.getOutputType();
        String outputPath = execution.getOutputFile();

        String outputString = toString(finalMap, execution.getMapKeys(), execution.getMemoryLevel());

        if(outputPath != null) {
            File outputFile = new File(outputPath);
            if(outputFile.isDirectory()) {
                TapLogger.error(TAG, "OutputFile is directory {}, which is unexpected, expect none or a file.", outputPath);
                return;
            }

            try(OutputStream fos = FileUtils.openOutputStream(outputFile)) {
                fos.write(outputString.getBytes(StandardCharsets.UTF_8));
            } catch(Throwable throwable) {
                throwable.printStackTrace();
                TapLogger.error(TAG, "Write output file {} failed, {}", outputPath, throwable.getMessage());
            }
        } else if(outputType.equalsIgnoreCase(Execution.OUTPUT_TYPE_TAP_LOGGER)){
            TapLogger.memory(TAG, outputString);
        }
    }

    private String toString(Map<String, MemoryFetcher> finalMap, List<String> mapKeys, String mapType) {
        if(mapType == null) {
            mapType = MemoryFetcher.MEMORY_LEVEL_IN_DETAIL;
        }
        StringBuilder builder = new StringBuilder("\r\n");
        for(Map.Entry<String, MemoryFetcher> entry : finalMap.entrySet()) {
            builder.append("---------------------------").append(entry.getKey()).append("---------------------------").append("\r\n");
            String finalMapType = mapType;
            CommonUtils.ignoreAnyError(() -> builder.append(entry.getValue().memory(mapKeys, finalMapType)).append("\r\n"), TAG);
            builder.append("---------------------------").append(entry.getKey()).append("---------------------------").append("\r\n").append("\r\n");
        }
        return builder.toString();
    }

    public void close() {
        if(isClosed.compareAndSet(false, true)) {
            for(ScheduledFuture<?> future : scheduledFutureList) {
                future.cancel(true);
            }
            scheduledFutureList.clear();
        }
    }
}
