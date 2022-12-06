package io.tapdata.pdk.core.memory;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MemoryManager {
    private static final String TAG = MemoryManager.class.getSimpleName();
    private String dumpingPath;
    private String commandPath;
    private String commandRunningPath;
    private String commandFailedPath;
    private String lastCommandPath;
    private Command runningCommand;
    private final ConcurrentHashMap<String, MemoryFetcher> keyMemoryFetcherMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RemovedMemoryFetcher> removedKeyMemoryFetcherMap = new ConcurrentHashMap<>();

    public static class RemovedMemoryFetcher {
        private final MemoryFetcher memoryFetcher;
        private final Long deleteTime;
        public RemovedMemoryFetcher(MemoryFetcher memoryFetcher, Long deleteTime) {
            this.memoryFetcher = memoryFetcher;
            this.deleteTime = deleteTime;
        }

        public MemoryFetcher getMemoryFetcher() {
            return memoryFetcher;
        }

        public Long getDeleteTime() {
            return deleteTime;
        }
    }

    private CommandWorker commandWorker;
    private MemoryManager() {
//        boolean dumpingEnabled = CommonUtils.getPropertyBool("memory_dumping_enable", true);
//        if(dumpingEnabled) {
//            dumpingPath = CommonUtils.getProperty("memory_output_dir", "./tap-dumping");
//            commandPath = CommonUtils.getProperty("memory_command_file", FilenameUtils.concat(dumpingPath, "./command"));
//            commandRunningPath = CommonUtils.getProperty("memory_command_running_file", FilenameUtils.concat(dumpingPath, "./command_running"));
//            commandFailedPath = CommonUtils.getProperty("memory_command_failed_file", FilenameUtils.concat(dumpingPath, "./command_failed"));
//            lastCommandPath = CommonUtils.getProperty("memory_command_last_file", FilenameUtils.concat(dumpingPath, "./command_last_time"));
//
//            File commandRunningFile = new File(commandRunningPath);
//            File commandFailedFile = new File(commandFailedPath);
//            File commandLastFile = new File(lastCommandPath);
//            if(commandLastFile.exists())
//                FileUtils.deleteQuietly(commandLastFile);
//            if(commandRunningFile.exists()) {
//                try {
//                    FileUtils.moveFile(commandRunningFile, commandLastFile);
//                } catch (Throwable ignored) {}
//            }
//            if(commandFailedFile.exists())
//                FileUtils.deleteQuietly(commandFailedFile);
//
//            ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(this::scanDir, 10, 10, TimeUnit.SECONDS);
//        }
        ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(this::cleanupRemovedMemoryFetchers, 30, 30, TimeUnit.SECONDS);
    }

    private void cleanupRemovedMemoryFetchers() {
        long time = System.currentTimeMillis();
        HashMap<String, RemovedMemoryFetcher> deletedEntryList = new HashMap<>();
        for(Map.Entry<String, RemovedMemoryFetcher> entry : removedKeyMemoryFetcherMap.entrySet()) {
            if(time > entry.getValue().deleteTime + 45000) {
                deletedEntryList.put(entry.getKey(), entry.getValue());
            }
        }

        for(Map.Entry<String, RemovedMemoryFetcher> entry : deletedEntryList.entrySet()) {
            removedKeyMemoryFetcherMap.remove(entry.getKey(), entry.getValue());
        }
    }

    private void scanDir() {
        File commandFile = new File(commandPath);
        if(commandFile.isFile()) {
            try {
                String commandContent = FileUtils.readFileToString(commandFile, "utf8");
                TapLogger.info(TAG, "Found command file to run at {}, modified time {} content {}", commandFile.getAbsolutePath(), commandFile.lastModified(), commandContent);
                Command newCommand = InstanceFactory.instance(JsonParser.class).fromJson(commandContent, Command.class);
                runningCommand = newCommand;
                File commandRunningFile = new File(commandRunningPath);
                if(commandRunningFile.exists())
                    FileUtils.deleteQuietly(commandRunningFile);
                FileUtils.moveFile(commandFile, new File(commandRunningPath));

                File commandFailedFile = new File(commandFailedPath);
                if(commandFailedFile.exists())
                    FileUtils.deleteQuietly(commandFailedFile);

                if(commandWorker != null) {
                    CommonUtils.ignoreAnyError(() -> {
                        commandWorker.close();
                    }, TAG);
                }
                commandWorker = new CommandWorker(runningCommand, keyMemoryFetcherMap);
//                    ExecutorsManager.getInstance().getExecutorService().execute(commandWorker);
                commandWorker.run();
            } catch (Throwable e) {
                e.printStackTrace();
                if(commandWorker != null) {
                    CommonUtils.ignoreAnyError(() -> {
                        commandWorker.close();
                    }, TAG);
                }

                TapLogger.error(TAG, "Scan dir {} failed, {}", commandPath, e.getMessage());
                File commandRunningFile = new File(commandRunningPath);
                File commandFailedFile = new File(commandFailedPath);
                if(commandRunningFile.exists())
                    FileUtils.deleteQuietly(commandRunningFile);
                if(commandFailedFile.exists())
                    FileUtils.deleteQuietly(commandFailedFile);
                try {
                    FileUtils.moveFile(commandFile, commandFailedFile);
                } catch (Throwable ignored) {
                }
                try(OutputStream fos = FileUtils.openOutputStream(commandFailedFile, true)) {
                    fos.write(("\r\n\r\n Error: " + ExceptionUtils.getStackTrace(e)).getBytes(StandardCharsets.UTF_8));
                } catch (Throwable ignored) {
                }
            }
        }
    }

    public MemoryManager register(String key, MemoryFetcher memoryFetcher) {
        if(key == null || memoryFetcher == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Illegal parameter for key {} memoryFetcher {} when register", key, memoryFetcher);
        keyMemoryFetcherMap.put(key, memoryFetcher);
        return this;
    }

    public MemoryFetcher unregister(String key) {
        if(key == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Illegal parameter for key when unregister");
        MemoryFetcher memoryFetcher = keyMemoryFetcherMap.remove(key);
        if(memoryFetcher != null) {
            removedKeyMemoryFetcherMap.put(key, new RemovedMemoryFetcher(memoryFetcher, System.currentTimeMillis()));
        }
        return memoryFetcher;
    }
    public static MemoryManager create() {
        return new MemoryManager();
    }

    public String output(List<String> keys) {
        return output(null, null);
    }
    public String output(List<String> keys, String keyRegex) {
        return output(keys, keyRegex, null);
    }
    public String output(List<String> keys, String keyRegex, String memoryLevel) {
        return new CommandWorker()
                .keyMemoryFetcherMap(keyMemoryFetcherMap)
                .removedKeyMemoryFetcherMap(removedKeyMemoryFetcherMap)
                .output(keys, keyRegex, memoryLevel);
    }

    public DataMap outputDataMap(List<String> keys, String keyRegex, String memoryLevel) {
        return new CommandWorker()
                .keyMemoryFetcherMap(keyMemoryFetcherMap)
                .removedKeyMemoryFetcherMap(removedKeyMemoryFetcherMap)
                .outputDataMap(keys, keyRegex, memoryLevel);
    }
}
