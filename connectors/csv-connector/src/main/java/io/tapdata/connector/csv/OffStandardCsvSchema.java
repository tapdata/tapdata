package io.tapdata.connector.csv;

import io.tapdata.common.FileSchema;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class OffStandardCsvSchema extends FileSchema {

    private final static String TAG = CsvSchema.class.getSimpleName();
    private final OffStandardFilter filter;

    public OffStandardCsvSchema(CsvConfig csvConfig, TapFileStorage storage) {
        super(csvConfig, storage);
        filter = new OffStandardFilter(csvConfig.getLineExpression());
    }

    public Map<String, Object> sampleFixedFileData(Map<String, TapFile> csvFileMap) throws Exception {
        Map<String, Object> sampleResult = new LinkedHashMap<>();
        String[] headers = fileConfig.getHeader().split(",");
        if (EmptyKit.isEmpty(csvFileMap)) {
            putIntoMap(headers, null, sampleResult);
        } else {
            for (String path : csvFileMap.keySet().stream().sorted().collect(Collectors.toList())) {
                AtomicBoolean needStop = new AtomicBoolean(false);
                storage.readFile(path, is -> {
                    try (
                            Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding());
                            BufferedReader bufferedReader = new BufferedReader(reader)
                    ) {
                        skipRead(bufferedReader, fileConfig.getDataStartLine() - 1);
                        String[] data = filter.filter(bufferedReader.readLine());
                        if (EmptyKit.isNotNull(data)) {
                            putIntoMap(headers, data, sampleResult);
                            needStop.set(true);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                if (needStop.get()) {
                    break;
                }
            }
        }
        return sampleResult;
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) throws Exception {
        storage.readFile(tapFile.getPath(), is -> {
            try (
                    Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding());
                    BufferedReader bufferedReader = new BufferedReader(reader)
            ) {
                if (fileConfig.getHeaderLine() > 0) {
                    skipRead(bufferedReader, fileConfig.getHeaderLine() - 1);
                    String[] headers = filter.filter(bufferedReader.readLine());
                    if (EmptyKit.isNull(headers)) {
                        return;
                    }
                    skipRead(bufferedReader, fileConfig.getDataStartLine() - fileConfig.getHeaderLine() - 1);
                    String[] data = filter.filter(bufferedReader.readLine());
                    putIntoMap(headers, data, sampleResult);
                } else {
                    skipRead(bufferedReader, fileConfig.getDataStartLine() - 1);
                    String[] data = filter.filter(bufferedReader.readLine());
                    if (EmptyKit.isNull(data)) {
                        return;
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
        });
    }

    private void skipRead(BufferedReader bufferedReader, int skip) throws IOException {
        while (skip > 0) {
            bufferedReader.readLine();
            skip--;
        }
    }
}
