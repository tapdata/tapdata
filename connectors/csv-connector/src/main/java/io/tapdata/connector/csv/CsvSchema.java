package io.tapdata.connector.csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CsvSchema {

    private final static String TAG = CsvSchema.class.getSimpleName();
    private final CsvConfig csvConfig;
    private final TapFileStorage storage;

    public CsvSchema(CsvConfig csvConfig, TapFileStorage storage) {
        this.csvConfig = csvConfig;
        this.storage = storage;
    }

    public Map<String, String> sampleFixedFileData(Map<String, TapFile> csvFileMap) throws Exception {
        Map<String, String> sampleResult = new LinkedHashMap<>();
        String[] headers = csvConfig.getHeader().split(csvConfig.getDelimiter());
        for (String path : csvFileMap.keySet().stream().sorted().collect(Collectors.toList())) {
            try (
                    Reader reader = new InputStreamReader(storage.readFile(path));
                    CSVReader csvReader = new CSVReaderBuilder(reader).build()
            ) {
                if (csvConfig.getIncludeHeader()) {
                    csvReader.skip(csvConfig.getDataStartLine());
                } else {
                    csvReader.skip(csvConfig.getDataStartLine() - 1);
                }
                String[] data = csvReader.readNext();
                if (EmptyKit.isNotNull(data)) {
                    putIntoMap(headers, data, sampleResult);
                    break;
                }
            }
        }
        return sampleResult;
    }

    public Map<String, String> sampleEveryFileData(ConcurrentMap<String, TapFile> csvFileMap) {
        Map<String, String> sampleResult = new LinkedHashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        CountDownLatch countDownLatch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                try {
                    TapFile file;
                    while ((file = getOutFile(csvFileMap)) != null) {
                        try (
                                Reader reader = new InputStreamReader(storage.readFile(file.getPath()));
                                CSVReader csvReader = new CSVReaderBuilder(reader).build()
                        ) {
                            csvReader.skip(csvConfig.getDataStartLine() - 1);
                            if (csvConfig.getIncludeHeader()) {
                                String[] headers = csvReader.readNext();
                                if (EmptyKit.isNull(headers)) {
                                    continue;
                                }
                                String[] data = csvReader.readNext();
                                putIntoMap(headers, data, sampleResult);
                            } else {
                                String[] data = csvReader.readNext();
                                if (EmptyKit.isNull(data)) {
                                    continue;
                                }
                                String[] headers = new String[data.length];
                                for (int j = 0; j < headers.length; j++) {
                                    headers[j] = "column" + (j + 1);
                                }
                                putIntoMap(headers, data, sampleResult);
                            }
                        } catch (Exception e) {
                            TapLogger.error(TAG, "read csv file error!", e);
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
        return sampleResult;
    }

    private synchronized TapFile getOutFile(ConcurrentMap<String, TapFile> csvFileMap) {
        if (EmptyKit.isNotEmpty(csvFileMap)) {
            String path = csvFileMap.keySet().stream().findFirst().orElseGet(String::new);
            TapFile tapFile = csvFileMap.get(path);
            csvFileMap.remove(path);
            return tapFile;
        }
        return null;
    }

    private void putIntoMap(String[] headers, String[] data, Map<String, String> sampleResult) {
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

    private synchronized void putValidIntoMap(Map<String, String> map, String key, String value) {
        if (!map.containsKey(key) || EmptyKit.isBlank(map.get(key))) {
            map.put(key, value);
        }
    }
}
