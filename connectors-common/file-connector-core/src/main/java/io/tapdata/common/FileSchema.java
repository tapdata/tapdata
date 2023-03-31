package io.tapdata.common;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public abstract class FileSchema {

    protected final FileConfig fileConfig;
    protected final TapFileStorage storage;

    public FileSchema(FileConfig fileConfig, TapFileStorage storage) {
        this.fileConfig = fileConfig;
        this.storage = storage;
    }

    public Map<String, Object> sampleEveryFileData(ConcurrentMap<String, TapFile> fileMap) {
        Map<String, Object> sampleResult = new LinkedHashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        CountDownLatch countDownLatch = new CountDownLatch(5);
        List<Exception> exceptionList = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                try {
                    TapFile file;
                    while ((file = getOutFile(fileMap)) != null) {
                        try {
                            sampleOneFile(sampleResult, file);
                        } catch (Exception e) {
                            exceptionList.add(e);
                        }
                    }
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        executorService.shutdown();
        if (EmptyKit.isNotEmpty(exceptionList)) {
            throw new RuntimeException("sample every file error: {}", exceptionList.get(0));
        }
        return sampleResult;
    }

    public Map<String, Object> sampleFixedFileData(Map<String, TapFile> csvFileMap) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected abstract void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) throws Exception;

    protected synchronized TapFile getOutFile(ConcurrentMap<String, TapFile> fileMap) {
        if (EmptyKit.isNotEmpty(fileMap)) {
            String path = fileMap.keySet().stream().findFirst().orElseGet(String::new);
            TapFile tapFile = fileMap.get(path);
            fileMap.remove(path);
            return tapFile;
        }
        return null;
    }

    protected void putIntoMap(String[] headers, String[] data, Map<String, Object> sampleResult) {
        if (EmptyKit.isNull(data)) {
            for (String header : headers) {
                putValidIntoMap(sampleResult, header, "");
            }
        } else {
            for (int i = 0; i < headers.length && i < data.length; i++) {
                putValidIntoMap(sampleResult, headers[i], data[i]);
            }
            for (int i = 0; i < headers.length - data.length; i++) {
                putValidIntoMap(sampleResult, headers[i + data.length], "");
            }
        }
    }

    protected synchronized void putValidIntoMap(Map<String, Object> map, String key, Object value) {
        if (!map.containsKey(key) || EmptyKit.isBlank((String) map.get(key))) {
            map.put(key, value);
        }
    }
}
