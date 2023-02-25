package io.tapdata.pdk.core.memory;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class CommandWorker implements Runnable {
    private static final String TAG = CommandWorker.class.getSimpleName();
    private Command command;
    private AtomicBoolean isClosed = new AtomicBoolean(false);
    private ConcurrentHashMap<String, MemoryManager.RemovedMemoryFetcher> removedKeyMemoryFetcherMap;
    public CommandWorker removedKeyMemoryFetcherMap(ConcurrentHashMap<String, MemoryManager.RemovedMemoryFetcher> removedKeyMemoryFetcherMap) {
        this.removedKeyMemoryFetcherMap = removedKeyMemoryFetcherMap;
        return this;
    }
    private ConcurrentHashMap<String, MemoryFetcher> keyMemoryFetcherMap;
    public CommandWorker keyMemoryFetcherMap(ConcurrentHashMap<String, MemoryFetcher> keyMemoryFetcherMap) {
        this.keyMemoryFetcherMap = keyMemoryFetcherMap;
        return this;
    }
    private List<ScheduledFuture<?>> scheduledFutureList = new CopyOnWriteArrayList<>();

    public CommandWorker() {}
    public CommandWorker(Command command, ConcurrentHashMap<String, MemoryFetcher> keyMemoryFetcherMap) {
        this.command = command;
        this.keyMemoryFetcherMap = keyMemoryFetcherMap;
    }


    @Override
    public void run() {
//        List<Execution> executionList = command.getExecutionList();
//        if(executionList == null || executionList.isEmpty()) {
//            TapLogger.warn(TAG, "No execution to do");
//            return;
//        }
//
//        for(final Execution execution : executionList) {
//            Runnable executionRunnable = () -> {
//                List<String> customScopes = execution.getCustomScopes();
//                Map<String, MemoryFetcher> finalMap = new HashMap<>();
//                if(customScopes != null && !customScopes.isEmpty()) {
//                    for(String customScope : customScopes) {
//                        MemoryFetcher fetcher = keyMemoryFetcherMap.get(customScope);
//                        if(fetcher != null) {
//                            finalMap.put(customScope, fetcher);
//                        }
//                    }
//                } else {
//                    finalMap.putAll(keyMemoryFetcherMap);
//                }
//                CommandWorker.this.output(finalMap, execution);
//            };
//            Integer intervalSeconds = execution.getExpectIntervalSeconds();
//            if(intervalSeconds != null && intervalSeconds > 0) {
//                scheduledFutureList.add(ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(executionRunnable, 1, intervalSeconds, TimeUnit.SECONDS));
//            } else {
//                executionRunnable.run();
//            }
//        }
    }

//    private void output(Map<String, MemoryFetcher> finalMap, Execution execution) {
//        String outputType = execution.getOutputType();
//        String outputPath = execution.getOutputFile();
//
//        String outputString = toString(finalMap, removedKeyMemoryFetcherMap, keys, execution.getKeyRegex(), execution.getMemoryLevel());
//
//        if(outputPath != null) {
//            File outputFile = new File(outputPath);
//            if(outputFile.isDirectory()) {
//                TapLogger.error(TAG, "OutputFile is directory {}, which is unexpected, expect none or a file.", outputPath);
//                return;
//            }
//
//            try(OutputStream fos = FileUtils.openOutputStream(outputFile)) {
//                fos.write(outputString.getBytes(StandardCharsets.UTF_8));
//            } catch(Throwable throwable) {
//                throwable.printStackTrace();
//                TapLogger.error(TAG, "Write output file {} failed, {}", outputPath, throwable.getMessage());
//            }
//        } else if(outputType.equalsIgnoreCase(Execution.OUTPUT_TYPE_TAP_LOGGER)){
//            TapLogger.memory(TAG, outputString);
//        }
//    }

    public String output(List<String> keys, String keyRegex) {
        return output(keys, keyRegex, null);
    }
    public String output(List<String> keys, String keyRegex, String memoryLevel) {
        return toString(keyMemoryFetcherMap, removedKeyMemoryFetcherMap, keys, keyRegex, memoryLevel);
    }

    public DataMap outputDataMap(List<String> keys, String keyRegex, String memoryLevel) {
        return toDataMap(keyMemoryFetcherMap, removedKeyMemoryFetcherMap, keys, keyRegex, memoryLevel);
    }

    private String toString(Map<String, MemoryFetcher> finalMap, ConcurrentHashMap<String, MemoryManager.RemovedMemoryFetcher> removedKeyMemoryFetcherMap, List<String> keys, String keyRegex, String memoryLevel) {
        DataMap allMap = toDataMap(finalMap, removedKeyMemoryFetcherMap, keys, keyRegex, memoryLevel);
        return JSON.toJSONString(allMap, true);
    }

    private DataMap toDataMap(Map<String, MemoryFetcher> finalMap, ConcurrentHashMap<String, MemoryManager.RemovedMemoryFetcher> removedKeyMemoryFetcherMap, List<String> keys, String keyRegex, String memoryLevel) {
        if(memoryLevel == null) {
            memoryLevel = MemoryFetcher.MEMORY_LEVEL_SUMMARY;
        }
        DataMap allMap = DataMap.create().keyRegex(keyRegex);
        for(Map.Entry<String, MemoryFetcher> entry : finalMap.entrySet()) {
            if(keys == null || matchKeys(keys, entry.getKey())) {
                allMap.kv(entry.getKey(), entry.getValue().memory(keyRegex, memoryLevel));
            }
        }
        DataMap removedAllMap = DataMap.create().keyRegex(keyRegex);
        for(Map.Entry<String, MemoryManager.RemovedMemoryFetcher> entry : removedKeyMemoryFetcherMap.entrySet()) {
            if(keys == null || matchKeys(keys, entry.getKey())) {
                DataMap memory = entry.getValue().getMemoryFetcher().memory(keyRegex, memoryLevel);
                if(memory != null) {
                    removedAllMap.kv(entry.getKey(),
                            map(
                                    entry("deleteTime", new Date(entry.getValue().getDeleteTime())),
                                    entry("memory", memory)
                            ));
                }
            }
        }
        allMap.kv("removed", removedAllMap);
        return allMap;
    }

    private boolean matchKeys(List<String> keys, String target) {
        if(keys == null)
            return false;
        for(String key : keys) {
            if(target.contains(key))
                return true;
        }
        return false;
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
